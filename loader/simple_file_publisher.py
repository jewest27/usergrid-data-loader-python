import json
from multiprocessing import Process

import sys
import datetime
import traceback
import logging
from logging.handlers import RotatingFileHandler

import argparse
import requests

MAX_SIZE = 262144

logger = logging.getLogger('EntityPublisher')

entity_url_template = '{url_base}/{org}/{app}/{collection}/{name}'


def init_logging(stdout_enabled=True):
    root_logger = logging.getLogger()
    log_file_name = './publisher.log'
    log_formatter = logging.Formatter(fmt='%(asctime)s | %(processName)s | %(name)s | %(levelname)s | %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')

    rotating_file = logging.handlers.RotatingFileHandler(filename=log_file_name,
                                                         mode='a',
                                                         maxBytes=104857600,
                                                         backupCount=10)
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
        logger.info(traceback.format_exc())
        raise e


def time_now():
    return unix_time(datetime.datetime.utcnow())


def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    td = dt - epoch

    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def parse_args():
    parser = argparse.ArgumentParser(description='Simple Usergrid Data Loader')

    parser.add_argument('--threads',
                        help='The number of threads to run over the file',
                        type=int,
                        default=4)

    parser.add_argument('-t', '--token',
                        help='The token which is used to authenticate requests to Usergrid',
                        type=str,
                        required=False)

    parser.add_argument('-f', '--filename',
                        help='The filename to load',
                        required=True,
                        type=str)

    parser.add_argument('-c', '--collection',
                        help='The collection to load into',
                        type=str)

    parser.add_argument('-o', '--org',
                        help='The org to load into',
                        type=str,
                        required=True)

    parser.add_argument('-a', '--app',
                        help='The org to load into',
                        type=str,
                        required=True)

    my_args = parser.parse_args(sys.argv[1:])

    logger.info(str(my_args))

    return vars(my_args)


def get_collection_from_filename(file_path):
    parts = file_path.split('/')

    filename = parts[len(parts) - 1]
    parts = filename.split('_')

    collection_name = parts[1]

    return collection_name


class FileProcessor(Process):
    def __init__(self, mod=1, num=1, **kwargs):
        super(FileProcessor, self).__init__()

        self.session = requests.Session()

        self.filename = kwargs.get('filename')
        self.collection_name = kwargs.get('collection')
        self.org_name = kwargs.get('org', 'missing-org')
        self.app_name = kwargs.get('app', 'missing-app')
        self.url_base = kwargs.get('url_base', 'https://api.usergrid.com')
        self.token = kwargs.get('token')

        if self.token is not None:
            self.session.headers.update({'authorization': 'Bearer %s' % self.token})

        self.mod = mod
        self.num = num

    def run(self):
        start_time = datetime.datetime.utcnow()

        total_lines = 0
        handled_lines = 0
        successes = 0
        errors = 0

        try:
            logger.info('Opening file {0}'.format(self.filename))

            for line in open(self.filename, 'r'):

                total_lines += 1

                if total_lines % 1000 == 1:
                    logger.info('Read line [%s]' % total_lines)

                line = line.rstrip().lstrip()

                if total_lines > 1 and (not line or line == ''):
                    logger.info('no line!')
                    break

                if len(line) > 0 and not line[0] == '{':
                    self.report_exception(line, 'line does not start with an open-bracket')
                    continue

                if total_lines % self.mod != self.num:
                    continue

                handled_lines += 1

                entity = None

                try:
                    entity = json.loads(line)
                except:
                    logger.error('Error parsing JSON on line [%s]: %s' % (total_lines, line))
                    continue

                if 'name' not in entity:
                    logger.error("Entity on line [%s] does not have a 'name': %s" % (total_lines, line))
                    continue

                entity_url = entity_url_template.format(url_base=self.url_base,
                                                        org=self.org_name,
                                                        app=self.app_name,
                                                        collection=self.collection_name,
                                                        name=entity.get('name'))

                r = self.session.put(entity_url, data=json.dumps(entity))

                if r.status_code == 200:
                    successes += 1
                else:
                    errors += 1
                    logger.error("Error [%s] on PUT entity to [%s] from line [%s]: %s" % (
                        r.status_code, entity_url, total_lines, r.text))

        except KeyboardInterrupt:
            logger.warn('terminating...')

        stop_time = datetime.datetime.utcnow()
        duration = stop_time - start_time

        logger.info('done! duration %s(s) total=%s, handled=%s, success=%s, errors=%s' % (duration, total_lines, handled_lines, successes, errors))

    def report_exception(self, line, message):

        message = {
            'line': line,
            'exception': message
        }

        # needs to be implemented


def main():
    init_logging(stdout_enabled=True)
    args = parse_args()
    threads = args.get('threads')

    if args.get('collection') is None:
        args['collection'] = get_collection_from_filename(args.get('filename'))

    processes = []

    try:
        processes = [FileProcessor(mod=threads, num=x, **args) for x in xrange(threads)]
        [p.start() for p in processes]

    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt.  Terminating...')
        [p.terminate() for p in processes]


if __name__ == '__main__':
    main()
