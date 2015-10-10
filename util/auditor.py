from Queue import Empty
import base64
import json
from multiprocessing import Process, JoinableQueue
import os
import traceback
import sys
import datetime
import time

import argparse
import boto
from boto.sqs.message import RawMessage
import requests


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def total_milliseconds(td):
    return (td.microseconds + td.seconds * 1000000) / 1000


def attach_queue(sqs_conn, queue_name):
    try:
        queue = sqs_conn.get_queue(queue_name)

        if not queue:
            queue = sqs_conn.create_queue(queue_name)

        return queue

    except Exception, e:
        print traceback.format_exc()
        raise e


def get_message_body(sqs_message):
    message_body = sqs_message.get_body()

    try:
        message = json.loads(message_body)
    except ValueError:
        message = json.loads(base64.b64decode(message_body))

    return message


class EntityLoader(Process):
    def __init__(self, config, delete_queue):
        super(EntityLoader, self).__init__()

        self.session_usergrid = requests.Session()

        self.sqs_config = config.get('sqs')

        self.delete_queue = delete_queue

        self.base_url = config.get('ug_base_url')

        self.usergrid_headers = {"Content-Type": "application/json"}

        self.credential_map = config.get('credential_map')
        self.queue_name = config.get('queue_name')

        elastic_config = config.get('elastic', {})

        self.check_name_exists = elastic_config.get('check_name_exists', False)
        self.check_queue_depth = elastic_config.get('check_queue_depth', False)
        self.queue_full_threshold = elastic_config.get('queue_threshold', 50000)
        self.queue_full_wait_time = elastic_config.get('queue_wait_time', 1000)
        self.es_index = elastic_config.get('index_name')
        self.es_url = elastic_config.get('es_url_base')
        self.es_index_url = '%s/%s' % (self.es_url, self.es_index)

        print 'connecting to queue %s...' % self.queue_name

        self.sqs_conn = boto.sqs.connect_to_region(**self.sqs_config)
        self.message_queue = self.sqs_conn.get_queue(self.queue_name)

        if self.message_queue is None:
            print 'Unable to connect to queue: %s' % self.queue_name
            exit(1)

        self.error_queue_name = self.queue_name + '-error'

        if self.error_queue_name[-12:] == '-error-error':
            self.error_queue_name = self.queue_name

        self.exception_queue_name = self.queue_name + '-exception'
        self.reprocess_queue_name = self.queue_name + '-reprocess'
        self.malformed_message_queue_name = self.queue_name + '-malformed'
        self.malformed_message_queue_name = self.queue_name + '-reprocess'

        self.error_queue = None
        self.reprocess_queue = None
        self.exception_queue = None
        self.parse_error_queue = None
        self.malformed_message_queue = None

    def check_exists(self, collection_name, entity_name):
        if not self.check_name_exists and self.es_url is not None:
            return False

        try:

            url = '%s/saparticles/_search' % self.es_index_url

            request = {
                "query": {
                    "filtered": {
                        "filter": {
                            "terms": {
                                "su_name": [entity_name]
                            }
                        }
                    }
                }
            }

            r = requests.get(url=url, data=json.dumps(request))

            # print 'ES: GET (%s) in %s: %s' % (r.status_code, total_milliseconds(r.elapsed), url)

            if not r.status_code == 200:
                print 'Unable to check if name exists!! (%s) %s' % (r.status_code, r.text)
                return False

            response = r.json()

            count = response.get('hits', {}).get('total', 0)

            return count >= 1

        except:
            print traceback.format_exc()
            return False

    def persist_entity(self, oac_tuple, the_entity, method='PUT'):
        # print 'Loading entity to Usergrid...'

        try:
            if method == 'PUT':
                url = self.get_entity_url(oac_tuple, the_entity)

                r = self.session_usergrid.put(url=url,
                                              data=json.dumps(the_entity),
                                              headers=self.usergrid_headers)
            else:
                url = self.get_collection_url(oac_tuple)

                r = self.session_usergrid.post(url=url,
                                               data=json.dumps(the_entity),
                                               headers=self.usergrid_headers)

            response_time = total_milliseconds(r.elapsed)

            if r.status_code != 200:
                print '%s (%s) in %s: %s' % (method, r.status_code, response_time, url)

            report_error = False
            message = ''

            if r.status_code != 200:
                report_error = True

                try:
                    message = r.json()

                except ValueError, e:
                    message = r.text

            else:
                try:
                    response = r.json()

                    if len(response.get('entities')) != 1:
                        message = 'Entity not returned after %s' % method
                        report_error = True

                except ValueError, e:
                    report_error = True
                    message = 'Exception (%s) getting JSON from response!' % e
                    print message
                    print 'response: %s' % r.text

                except Exception, e:
                    report_error = True
                    message = 'Exception (%s) getting JSON from response!' % e
                    print message
                    print 'response: %s' % r.text

            if report_error:
                self.post_error(oac_tuple, the_entity, message)

            return response_time

        except Exception, e:
            self.post_exception(oac_tuple, the_entity, 'Error on %s: %s' % (method, traceback.format_exc()))
            print traceback.format_exc()
            raise e

    def put_entities_to_usergrid(self, oac_tuple, entity_wrappers):

        try:
            collection_url = self.get_collection_url(oac_tuple)

            print 'Loading %s to collection %s URL: %s' % (len(entity_wrappers), oac_tuple, collection_url)

            for entity_wrapper in entity_wrappers:

                r = requests.put(url=collection_url,
                                 data=json.dumps(entity_wrapper),
                                 headers=self.usergrid_headers)

                if r.status_code != 200:
                    self.post_error(oac_tuple, entity_wrapper, r.json())

        except Exception, e:
            self.post_exception(oac_tuple, entity_wrappers, traceback.format_exc())
            print traceback.format_exc()
            raise e

    def post_entities_to_usergrid(self, oac_tuple, the_entities):

        try:
            collection_url = self.get_collection_url(oac_tuple)

            # print 'Loading %s to collection %s URL: %s' % (len(the_entities), oac_tuple, collection_url)

            r = requests.post(url=collection_url,
                              data=json.dumps(the_entities),
                              headers=self.usergrid_headers)

            response = r.json()

            if r.status_code != 200:
                self.post_error_array(oac_tuple, the_entities, r.json())

            elif response is not None:
                if len(response.get('entities')) != len(the_entities):
                    print 'Wrong Entity Count from post!  %s not %s' % (
                        len(response.get('entities')), len(the_entities))

                    entity_map = {}

                    for response_entity in response.get('entities'):
                        entity_map[response_entity.get('name')] = response_entity

                    if the_entities:
                        for entity in the_entities:
                            if entity and entity.get('name') not in entity_map:
                                self.persist_entity(oac_tuple, entity, method='POST')
            else:
                raise Exception('Unexpected - JW')

        except Exception, e:
            self.post_exception(oac_tuple, the_entities, e)
            print traceback.format_exc()
            raise e

    def check_queues_and_wait(self):
        if not self.check_queue_depth and self.es_url is not None:
            return

        should_continue = False

        while not should_continue:
            url = '%s/_cat/thread_pool?v=&h=iq' % self.es_url
            # print url

            response = requests.get(url)

            # print response.text

            lines = response.text.split('\n')

            total_queued = 0L
            line_counter = 0

            for line in lines:

                line_counter += 1

                if line_counter == 1:
                    continue

                if line == '':
                    continue

                try:
                    line = line.rstrip().lstrip()
                    # print '[%s]' % line
                    total_queued += long(line)
                except:
                    continue

            if total_queued > self.queue_full_threshold:
                print 'MAX QUEUED (%s) MET.  Sleeping...' % total_queued
                time.sleep(self.queue_full_wait_time)
                should_continue = False
            else:
                if total_queued > 0:
                    print 'ES INDEX Nodes: %s Queue Size: %s' % (len(lines) - 1, total_queued)

                should_continue = True

    def run(self):
        message_counter = 0L
        start_time = datetime.datetime.utcnow()
        sqs_iterations = 0
        sqs_total_response = 0

        total_response_time = 0

        while True:
            self.check_queues_and_wait()

            sqs_start_time = datetime.datetime.utcnow()

            # print 'querying SQS...'
            sqs_messages = self.message_queue.get_messages(num_messages=10,
                                                           wait_time_seconds=10)
            sqs_stop_time = datetime.datetime.utcnow()
            sqs_iterations += 1
            sqs_response_time = total_milliseconds(sqs_stop_time - sqs_start_time)
            sqs_total_response += sqs_response_time

            if message_counter > 0:
                print '%s: got message count: %s | SQS response time: %sms  | SQS avg response time: %sms | BaaS Avg Response %sms' % (
                    datetime.datetime.utcnow(), len(sqs_messages), sqs_response_time,
                    (sqs_total_response / sqs_iterations),
                    (total_response_time / message_counter))

            else:
                print '%s: got message count: %s | SQS response time: %sms  | SQS avg response time: %sms' % (
                    datetime.datetime.utcnow(), len(sqs_messages), sqs_response_time,
                    (sqs_total_response / sqs_iterations))

            for sqs_message in sqs_messages:

                message_counter += 1
                # print 'Counter: %s' % message_counter

                try:
                    message = get_message_body(sqs_message)

                    if not 'org' in message or not 'app' in message or not 'collection' in message or not 'entity' in message:
                        self.post_malformed_message_error(sqs_message)
                        self.delete_queue.put(sqs_message)
                        continue

                    collection = message.get('collection')
                    entity = message.get('entity')

                    if 'name' in entity:

                        if not self.check_exists(collection, entity.get('name')):
                            self.post_to_reprocess(message)

                    self.delete_queue.put(sqs_message)

                except Exception, e:
                    print traceback.format_exc()

            if len(sqs_messages) == 0:
                time.sleep(1)

            # after loop
            now_time = datetime.datetime.utcnow()
            duration_delta = now_time - start_time
            duration = total_seconds(duration_delta)

            if duration > 0 and message_counter % 100 == 0:
                print 'counter: %s eps: %s' % (message_counter, 1.0 * message_counter / duration)

    def post_error(self, oac_tuple, the_entity, response_message):
        message = {
            'org': oac_tuple.get('org'),
            'app': oac_tuple.get('app'),
            'collection': oac_tuple.get('collection'),

            'response/message': response_message,

            'entity': the_entity
        }

        m = RawMessage()
        m.set_body(json.dumps(message, 2))

        if self.error_queue is None:
            self.error_queue = attach_queue(self.sqs_conn, self.error_queue_name)

        self.error_queue.write(m)

    def post_exception(self, oac_tuple, the_entity, traceback_exception):
        message = {
            'org': oac_tuple.get('org'),
            'app': oac_tuple.get('app'),
            'collection': oac_tuple.get('collection'),

            'exception': str(traceback_exception),

            'entity': the_entity
        }

        m = RawMessage()
        m.set_body(json.dumps(message, 2))

        if self.exception_queue is None:
            self.exception_queue = attach_queue(self.sqs_conn, self.exception_queue_name)

        self.exception_queue.write(m)

    def post_error_array(self, oac_tuple, the_entities, response):
        for entity in the_entities:
            self.post_error(oac_tuple, entity, response)

    def post_exception_array(self, oac_tuple, the_entities, response):

        for entity in the_entities:
            self.post_exception(oac_tuple, entity, response)

    def get_collection_url(self, oac_tuple):
        auth_params = self.get_org_credentials(org=oac_tuple.get('org'))

        params = {
            'org': oac_tuple.get('org'),
            'app': oac_tuple.get('app'),
            'collection': oac_tuple.get('collection'),
            'client_id': auth_params.get('client_id'),
            'client_secret': auth_params.get('client_secret')
        }

        return self.base_url + '/{org}/{app}/{collection}?client_id={client_id}&client_secret={client_secret}'.format(
            **params)

    def get_entity_url(self, oac_tuple, the_entity):
        auth_params = self.get_org_credentials(org=oac_tuple.get('org'))

        params = {
            'org': oac_tuple.get('org'),
            'app': oac_tuple.get('app'),
            'collection': oac_tuple.get('collection'),
            'name': the_entity.get('name'),
            'client_id': auth_params.get('client_id'),
            'client_secret': auth_params.get('client_secret')
        }

        return self.base_url + '/{org}/{app}/{collection}/{name}?client_id={client_id}&client_secret={client_secret}'.format(
            **params)

    def get_org_credentials(self, org):

        if not org in self.credential_map:
            raise Exception('Credentials not found for org=[%s]' % org)

        return self.credential_map.get(org)

    def post_malformed_message_error(self, sqs_message):
        pass

    def post_to_reprocess(self, message):

        m = RawMessage()
        m.set_body(json.dumps(message, 2))

        if self.reprocess_queue is None:
            self.reprocess_queue = attach_queue(self.sqs_conn, self.reprocess_queue_name)

        self.reprocess_queue.write(m)


def time_now():
    return unix_time(datetime.datetime.utcnow())


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    td = dt - epoch

    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def parse_args():
    parser = argparse.ArgumentParser(description='Apigee BaaS Loader - Publisher')

    parser.add_argument('-q', '--queue_name',
                        help='The queue to load into',
                        required=True,
                        type=str,
                        default='entity-queue')

    parser.add_argument('--config',
                        help='The queue to load into',
                        type=str,
                        default=os.path.join(os.environ.get('HOME'), '.usergrid/example.json'))

    parser.add_argument('-t', '--threads',
                        help='The number of threads to run',
                        type=int,
                        default=8)

    my_args = parser.parse_args(sys.argv[1:])

    print str(my_args)

    return vars(my_args)


class MessageDeleter(Process):
    def __init__(self, sqs_config, queue_name, delete_queue):
        super(MessageDeleter, self).__init__()
        self.queue_name = queue_name
        self.sqs_config = sqs_config
        self.delete_queue = delete_queue

    def run(self):
        print 'MessageDeleter connecting to queue %s...' % self.queue_name
        sqs_conn = boto.sqs.connect_to_region(**self.sqs_config)
        message_queue = sqs_conn.get_queue(self.queue_name)
        counter = 0

        while True:
            try:
                sqs_message = self.delete_queue.get(timeout=120)

                if sqs_message:
                    counter += 1
                    message_queue.delete_message(sqs_message)

                self.delete_queue.task_done()

            except Empty:
                print 'Nothing to delete...'


def main():
    args = parse_args()

    print 'Using config file: %s' % args['config']

    with open(args['config'], 'r') as f:
        config = json.load(f)

    if config is None:
        print 'Error loading config!'
        exit()

    sqs_config = config.get('sqs')

    delete_queue = JoinableQueue()

    delete_threads = [MessageDeleter(queue_name=args.get('queue_name'),
                                     sqs_config=sqs_config,
                                     delete_queue=delete_queue) for x in xrange(4)]

    [deleter.start() for deleter in delete_threads]
    config.update(args)

    workers = [EntityLoader(config=config,
                            delete_queue=delete_queue) for x in xrange(args.get('threads'))]

    [worker.start() for worker in workers]

    keep_going = True

    while keep_going:
        try:
            time.sleep(60)

        except KeyboardInterrupt:
            keep_going = False
            print 'terminating...'
            [worker.terminate() for worker in workers]
            [deleter.terminate() for deleter in delete_threads]


if __name__ == '__main__':
    main()
