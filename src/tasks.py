# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import sys
import json
import logging
import traceback
from urllib import urlencode
from datetime import datetime, timedelta

import requests
import pymongo
from celery import Celery
from kombu import Exchange, Queue
from bson import ObjectId

import configs

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s][%(name)s:%(process)d][%(asctime)s]: %(message)s', handlers=[
                    logging.StreamHandler(), ])

db = pymongo.MongoClient(configs.MONGO_URI)[configs.DATABASE_NAME]
celery = Celery(
    'tasks', backend=configs.REDIS_URI, broker=configs.REDIS_URI)

celery.conf.CELERY_TASK_RESULT_EXPIRES = None
celery.conf.CELERY_IGNORE_RESULT = True
celery.conf.CELERY_TIMEZONE = 'Asia/Shanghai'
celery.conf.CELERYD_TASK_SOFT_TIME_LIMIT = 300

celery.conf.CELERYBEAT_SCHEDULE = {
    'cron-tasks-every-second': {
        'task': 'hancock.tasks.hotairline',
        'schedule': timedelta(seconds=1)
    },
}

celery.conf.CELERY_QUEUES = (
    Queue('default', Exchange('default'), routing_key='default'),
    Queue('phantomjs', Exchange('phantomjs'), routing_key='phantomjs'),
    Queue('cron', Exchange('cron'), routing_key='cron'),
)

CURRENT_HOTAIRLINE_ID = None


def _get_template_str(template_name):
    with open('templates/{}.js'.format(template_name)) as f:
        return ''.join([x for x in f.readlines()])


@celery.task(name='hancock.tasks.hotairline_phantomjs', queue='phantomjs', routing_key='phantomjs')
def hotairline_phantomjs(info):
    start_city, dest_city = info['start_city'], info['dest_city']
    now = datetime.utcnow()
    for x in range(1):
        for web in configs.HOTAIRLINE_WEBSITES_PHANTOMJS:
            info['start_date'] = (now + timedelta(days=1)).strftime('%Y-%m-%d')
            info['back_date'] = (now + timedelta(days=3)).strftime('%Y-%m-%d')

            params = {}
            for i, j in web['params']:
                params[i] = info[j] if info.get(j) else j
            payload = {
                'url': web['host'] + '?' + urlencode(params),
                'proxy': '',
                'js_script': _get_template_str(web['template'])
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept-Encoding': 'identity, deflate, compress, gzip'
            }
            res = requests.post(
                configs.PHANTOMJS_SERVER, data=json.dumps(payload), headers=headers)
            if res.status_code == 200:
                result = res.json()
                if result.get('status_code') == 200:
                    r = result.get('js_script_result')
                    if r:
                        pass
            else:
                pass


@celery.task(name='hancock.tasks.hotairline', queue='cron', routing_key='cron')
def hotairline():
    global CURRENT_HOTAIRLINE_ID
    _q = {
        '_id': {'$lt': ObjectId(CURRENT_HOTAIRLINE_ID)}} if CURRENT_HOTAIRLINE_ID else {}
    airlines = db['hotairline'].find(_q).sort([('_id', -1)]).limit(1)
    if airlines.count() == 0:
        CURRENT_HOTAIRLINE_ID = None
    for airline in airlines:
        hotairline_phantomjs.delay(airline)

if __name__ == "__main__":
    celery.start()
