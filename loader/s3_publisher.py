from Queue import Empty
import json
from multiprocessing import Process, JoinableQueue
import os
import smtplib
import sys
import datetime
import time
import traceback
import logging
from logging.handlers import RotatingFileHandler
import uuid
import zlib

import argparse
import boto
from boto.sqs.message import RawMessage
import requests

STATUS_STAGING_TO_SQS = 'Loading to BaaS'
STATUS_COMPLETE = 'Loading Complete'

BATCH_SIZE = 10
MAX_SIZE = 262144
SKIP_SQS = False

top_logger = logging.getLogger('S3Publisher')


def get_metadata(keyString):
    parts = keyString.split('/')
    filename = parts[len(parts) - 1]
    date_dir = parts[len(parts) - 2]

    return {
        'filename': filename,
        'date': date_dir
    }


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './publisher.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(processName)s | %(name)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=104857600,
                                                         backupCount=10)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    rotating_file.setFormatter(log_formatter)
    rotating_file.setLevel(logging.INFO)

    root_logger.addHandler(rotating_file)
    root_logger.setLevel(logging.INFO)

    if stdout_enabled:
        stdout_logger = logging.StreamHandler(sys.stdout)
        stdout_logger.setFormatter(log_formatter)
        stdout_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_logger)


def attach_queue(sqs_conn, queue_name):
    try:
        queue = sqs_conn.get_queue(queue_name)

        if not queue:
            queue = sqs_conn.create_queue(queue_name)

        return queue

    except Exception, e:
        raise e


def time_now():
    return unix_time(datetime.datetime.utcnow())


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    td = dt - epoch

    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def parse_args():
    parser = argparse.ArgumentParser(description='Apigee BaaS Loader - Publisher')

    parser.add_argument('--s3_config_file',
                        help='The queue to load into',
                        type=str,
                        default='%s/.usergrid/s3_config.json' % os.getenv("HOME"))

    parser.add_argument('-t', '--threads',
                        help='The number of threads to run over the file',
                        type=int,
                        default=16)

    parser.add_argument('-k', '--keystring',
                        help='The specific key to process',
                        type=str)

    parser.add_argument('-q', '--queue_name',
                        help='The queue name to send messages to.  If not specified the filename is used',
                        default='entities',
                        type=str)

    parser.add_argument('--bucket',
                        help='The dir to place the downloaded files',
                        required=True)

    parser.add_argument('--sqs_config_file',
                        help='The number of threads to run over the file',
                        type=str,
                        default='%s/.usergrid/sqs_config.json' % os.getenv("HOME"))

    parser.add_argument('-o', '--org',
                        help='The target org',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='The target app',
                        type=str,
                        required=True)

    my_args = parser.parse_args(sys.argv[1:])

    top_logger.info(str(my_args))

    return vars(my_args)


def get_collection_from_filename(file_path):
    parts = file_path.split('/')

    filename = parts[len(parts) - 1]
    parts = filename.split('_')

    collection_name = parts[1]

    return collection_name


def balanced_braces(line):
    counter = 0

    for ch in line:
        if ch == '{':
            counter += 1
        elif ch == '}':
            counter -= 1

    return counter == 0


class SQSPublisher(Process):
    def __init__(self, **kwargs):
        super(SQSPublisher, self).__init__()
        self.logger = logging.getLogger('SQSPublisher')
        self.sqs_config = kwargs.get('sqs_config')
        self.sqs_conn = boto.sqs.connect_to_region(**self.sqs_config)
        self.publish_queue = kwargs.get('publish_queue')
        self.queue_name = kwargs.get('queue_name')
        self.repair_queue_name = self.queue_name + '-repair'
        self.done = False

    def run(self):
        self.logger.info('Connecting to queue {0}'.format(self.queue_name))

        batch = []

        repair_queue = attach_queue(self.sqs_conn, self.repair_queue_name)
        queue = attach_queue(self.sqs_conn, self.queue_name)

        if not queue:
            raise ValueError('cannot bind to queue: %s' % self.queue_name)

        counter = 0
        empty_counter = 0

        while not self.done:
            try:
                message = self.publish_queue.get(timeout=60)

                counter += 1

                if len(str(batch)) + len(str(message)) < MAX_SIZE:
                    batch.append((str(uuid.uuid1()), json.dumps(message), 0))

                else:
                    self.logger.debug(
                        'Write_batch key={key} org={org} app={app} col={collection} BATCH_SIZE={BATCH_SIZE}'
                        ' size={size} counter={counter}'.format(
                            BATCH_SIZE=BATCH_SIZE, size=len(batch), counter=counter, **message))

                    queue.write_batch(batch)
                    repair_queue.write_batch(batch)
                    batch = [(str(uuid.uuid1()), json.dumps(message), 0)]

                if len(batch) >= BATCH_SIZE:
                    self.logger.debug(
                        'Write_batch key={key} org={org} app={app} col={collection} BATCH_SIZE={BATCH_SIZE}'
                        ' size={size} counter={counter}'.format(
                            BATCH_SIZE=BATCH_SIZE, size=len(batch), counter=counter, **message))

                    queue.write_batch(batch)
                    repair_queue.write_batch(batch)
                    batch = []

                self.publish_queue.task_done()

            except Empty, e:
                empty_counter += 1
                self.logger.warn('EMPTY!  Count: %s' % empty_counter)

                if empty_counter >= 5:
                    self.done = True


class S3Processor(Process):
    def __init__(self, **kwargs):
        super(S3Processor, self).__init__()
        self.logger = logging.getLogger('S3Processor')
        self.args = kwargs

        self.sqs_config = kwargs.get('sqs_config')
        self.sqs_conn = boto.sqs.connect_to_region(**self.sqs_config)

        self.key_name = kwargs.get('key')
        self.org_name = kwargs.get('org', 'missing-org')
        self.app_name = kwargs.get('app', 'missing-app')

        self.s3_config = kwargs.get('s3_config')
        self.publish_queue = kwargs.get('publish_queue')

        self.queue_name = kwargs.get('queue_name')
        self.parse_error_queue_name = self.queue_name + '-parse-errors'

    def run(self):
        start_time = datetime.datetime.utcnow()

        self.logger.info('Starting work...')

        counter = 0
        last_line = ''
        line = ''
        try:
            s3_conn = boto.connect_s3(
                self.s3_config.get('aws_access_key_id'),
                self.s3_config.get('aws_secret_access_key'))

            bucket = s3_conn.get_bucket(self.args.get('bucket'))

            key_list = bucket.list()

            for key in key_list:

                if key.key == self.key_name:
                    collection = get_collection_from_filename(self.key_name)
                    self.logger.info('Found collection=[%s] for key=[%s]' % (collection, key.key))

                    carry_line = ''

                    for data in self.stream_gzip_decompress(key):
                        lines = data.split('\n')
                        line_counter = 0

                        for line in lines:
                            line = carry_line + line

                            if len(line) > 0 and line[0] != '{':
                                carry_line = ''
                                self.logger.info('Bad Line: %s' % line)
                                self.report_parse_exception(line, 'Bad Line')
                                last_line = line
                                continue

                            line_counter += 1
                            counter += 1

                            try:
                                entity = json.loads(line.replace('\\', '\\\\'))
                                carry_line = ''

                                message = {
                                    'org': self.org_name,
                                    'app': self.app_name,
                                    'collection': collection,
                                    'entity': entity,
                                    'key': key.key
                                }

                                self.publish_queue.put(message)

                            except ValueError, e:
                                last_line = line

                                if line_counter == len(lines):
                                    counter -= 1
                                    carry_line = line

                                else:
                                    if balanced_braces(line):
                                        carry_line = ''
                                        self.report_parse_exception(line, traceback.format_exc())
                                    else:
                                        carry_line = line

        except KeyboardInterrupt, e:
            raise e

        finally:
            self.logger.warn('terminating...')

            stop_time = datetime.datetime.utcnow()
            duration = stop_time - start_time

            self.logger.info('done! duration %s(s) count: %s' % (duration, counter))

    def stream_gzip_decompress(self, stream):
        dec = zlib.decompressobj(32 + zlib.MAX_WBITS)  # offset 32 to skip the header
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv

    def report_parse_exception(self, line, message):

        message = {
            'line': line,
            'exception': message,
        }

        queue = attach_queue(self.sqs_conn, self.parse_error_queue_name)

        if queue:
            m = RawMessage()
            m.set_body(json.dumps(message))
            queue.write(m)


def check_processed(keyString):
    meta = get_metadata(keyString)

    # todo: get from config file

    params = {
        'url_base': 'http://api.usergrid.com',
        'org': 'myOrg',
        'app': 'myApp',
        'collection': 'bulkloader',
        'name': meta.get('date'),
        'client_id': '<usergrid client_id>',
        'client_secret': '<usergrid client_secret>'
    }

    url = '{url_base}/{org}/{app}/{collection}/{name}?client_id={client_id}&client_secret={client_secret}'.format(
        **params)

    r = requests.put(url, timeout=30, data='{"temp": false}')

    if r is None or r.status_code != 200:
        raise Exception('PUT: unable to determine current state of loading! response: ' + r.text)

    r = requests.get(url, timeout=30)
    time.sleep(2)

    if r is None or r.status_code != 200:
        raise Exception('GET: unable to determine current state of loading! response: ' + r.text)

    response_json = r.json()

    if len(response_json.get('entities', [])) > 0:
        entity = response_json.get('entities')[0]

        return keyString in entity

    return False


def update_status(keyString, status):
    meta = get_metadata(keyString)

    params = {
        'url_base': 'http://api.usergrid.com',
        'org': 'myOrg',
        'app': 'myApp',
        'collection': 'bulkloader',
        'name': meta.get('date'),
        'client_id': '<usergrid client_id>',
        'client_secret': '<usergrid client_secret>'
    }

    url = '{url_base}/{org}/{app}/{collection}/{name}?client_id={client_id}&client_secret={client_secret}'.format(
        **params)

    data = {
        keyString: {
            'status': status,
            'date_time': time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        }
    }

    r = requests.put(url, data=json.dumps(data), timeout=30)

    if r.status_code == 200:
        return True
    else:
        return False


def get_date_keys():
    date_spec = "%Y%m%d"
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)

    return [
        # today.strftime(date_spec),
        tomorrow.strftime(date_spec)
    ]


def notify_status(new_keys, status):
    if len(new_keys) == 0:
        return

    status_message_map = {
        STATUS_STAGING_TO_SQS: "New files have been detected in S3 and are being staged in SQS.  They will start "
                               "being loaded to BaaS within the next 10 minutes.",

        STATUS_COMPLETE: "The SQS queue has been drained and the files below should be completely loaded into BaaS."
    }

    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()

        # message = 'The following S3 files are now [%s]: \r\n\r\n' % status
        message = status_message_map.get(status)

        files_str = '\r\n\r\nFiles: \r\n\r\n'

        for file in new_keys:
            files_str += '- '
            files_str += file
            files_str += '\r\n'

        message += files_str

        msg = "\r\n".join([
            "From: example@usergrid.com",
            "To: example@usergrid.com",
            "Subject: BaaS Loader Update: %s files %s" % (len(new_keys), status),
            "",
            "%s" % message
        ])

        server.login('example@usergrid.com', 'password')

        top_logger.info('sending mail keys=[%s] status=[%s]' % (new_keys, status))

        server.sendmail('example@usergrid.com', ['example@usergrid.com', 'example@usergrid.com'], msg)

        server.close()
        top_logger.info('mail sent successfully for keys=[%s] status=[%s]' % (new_keys, status))

    except Exception, e:
        print traceback.format_exc()
        top_logger.info('unable to send mail for keys=[%s] status=[%s]' % (new_keys, status))


def main():
    init_logging(stdout_enabled=True)
    args = parse_args()
    threads = args.get('threads')

    strings = get_date_keys()

    top_logger.info('Using date keys: %s' % strings)

    with open(args.get('sqs_config_file'), 'r') as f:
        sqs_config = json.load(f)

    sqs_conn = boto.sqs.connect_to_region(**sqs_config)
    queue_name = args.get('queue_name')
    queue = attach_queue(sqs_conn, queue_name)

    if not queue:
        queue = sqs_conn.create_queue(queue_name)

        if not queue:
            top_logger.info('unable to bind to queue %s' % queue_name)
            exit(1)

    with open(args.get('s3_config_file'), 'r') as f:
        s3_config = json.load(f)

    s3_conn = boto.connect_s3(
        s3_config.get('aws_access_key_id'),
        s3_config.get('aws_secret_access_key'))

    bucket = s3_conn.get_bucket(args.get('bucket'))

    key_list = bucket.list()
    processors = []
    publishers = []
    new_keys = []
    publish_queue = JoinableQueue()

    try:
        for key in key_list:
            keyString = key.key

            if keyString[-1:] == '/':
                continue

            process = False

            if args.get('keystring') is not None:
                if args.get('keystring') in keyString:
                    process = True
            else:
                for string in strings:
                    if string in keyString:
                        top_logger.info('Found [%s] in Key [%s]: Evaluating...' % (string, keyString))
                        process = True
                        break

            if not process:
                continue

            if keyString[-3:] == '.gz' and ('put' in keyString or 'post' in keyString):

                already_processed = check_processed(keyString)

                if not already_processed:
                    new_keys.append(keyString)

                    top_logger.info('Starting S3Processor for Key=%s' % keyString)

                    processor = S3Processor(key=key.key,
                                            s3_config=s3_config,
                                            sqs_config=sqs_config,
                                            publish_queue=publish_queue,
                                            **args)

                    processor.start()
                    processors.append(processor)

            else:
                top_logger.info('Skipping Key: %s - not gzip!' % keyString)

        if not len(new_keys) > 0:
            top_logger.warn('No New files!')

        else:
            publishers = [SQSPublisher(sqs_config=sqs_config,
                                       publish_queue=publish_queue,
                                       **args)

                          for x in xrange(threads)]

            [p.start() for p in publishers]

            for new_key in new_keys:
                update_status(new_key, STATUS_STAGING_TO_SQS)

            notify_status(new_keys, STATUS_STAGING_TO_SQS)

            top_logger.info('Monitoring Publishers...')

            publishers_finished = False
            active_publisher_count = 0

            while not publishers_finished:
                publishers_finished = True
                active_publisher_count = 0

                for p in publishers:
                    if p.is_alive():
                        active_publisher_count += 1
                        publishers_finished = False

                if publishers_finished is False:
                    top_logger.warn('There are still [%s] Publishers active, sleeping...' % active_publisher_count)
                    time.sleep(5)
                    top_logger.warn('Awake!')

            top_logger.info('PublishersComplete!')

            top_logger.info('Connecting to SQS...')
            sqs_conn = boto.sqs.connect_to_region(**sqs_config)

            queue_name = args.get('queue_name')
            top_logger.info('Connecting to queue [%s]...' % queue_name)
            queue = sqs_conn.get_queue(queue_name)

            count = 1
            top_logger.info('Starting loop to check for completion')
            count_with_zero = 0
            try:
                while count_with_zero < 10:

                    top_logger.info('Completion Monitor Loop: Count [%s]' % count)
                    time.sleep(10)
                    count = queue.count()

                    if count > 0:
                        count_with_zero = 0
                        top_logger.info('Queue=[%s] has [%s] messages.  Waiting...' % (queue_name, count))
                    else:
                        count_with_zero += 1
                        if count_with_zero >= 10:
                            top_logger.info('Queue=[%s] IS EMPTY! Finishing...' % queue_name)

                for new_key in new_keys:
                    update_status(new_key, STATUS_COMPLETE)

                notify_status(new_keys, STATUS_COMPLETE)

            except Exception, e:
                print traceback.format_exc()
                raise e

    except KeyboardInterrupt:
        top_logger.info('Keyboard Interrupt.  Terminating...')
        [p.terminate() for p in processors]
        [p.terminate() for p in publishers]


if __name__ == '__main__':
    main()
