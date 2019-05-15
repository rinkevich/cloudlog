
import atexit
import enum
import hashlib
import logging
import marshal
import re
import struct
import threading
import time
from collections import deque
from queue import Empty, Queue

import boto3
import six

logger = logging.getLogger(__name__)


class CloudWatchLogEventStorage(deque):

    def __init__(self, client, group_name, stream_name, flush_interval=10,
                 flush_threshold=1000, persistent=False, *args, **kwargs):
        super(CloudWatchLogEventStorage, self).__init__(*args, **kwargs)
        self._client = client
        self._group_name = group_name
        self._stream_name = stream_name
        self._flush_interval = flush_interval
        self._flush_threshold = flush_threshold
        self._last_flush_timestamp = time.time()
        self._sequence_token = None
        self._publish_thread = None

        self._persistent_storage = persistent
        # TODO: save logs to local files

    @property
    def log_path(self):
        return f'{self._group_name}/{self._stream_name}'

    @property
    def publisher(self):
        return self._publish_thread

    def _get_sequence_token(self, create_bucket=False):
        if create_bucket:
            try:
                self._client.create_log_group(logGroupName=self._group_name)
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass

            try:
                self._client.create_log_stream(
                    logGroupName=self._group_name,
                    logStreamName=self._stream_name)
                sequence_token = None
            except self._client.exceptions.ResourceAlreadyExistsException:
                response = self._client.describe_log_streams(
                    logGroupName=self._group_name,
                    logStreamNamePrefix=self._stream_name,
                    orderBy='LogStreamName', descending=False, limit=1)
                sequence_token = response['logStreams'][0] \
                    .get('uploadSequenceToken')
            return sequence_token
        else:
            response = self._client.describe_log_streams(
                logGroupName=self._group_name,
                logStreamNamePrefix=self._stream_name,
                orderBy='LogStreamName', descending=False, limit=1)
            sequence_token = response['logStreams'][0] \
                .get('uploadSequenceToken')
            return sequence_token

    def _publish_records(self, records_batch):
        if not self._sequence_token:
            self._sequence_token = self._get_sequence_token(create_bucket=True)

        kwargs = {}
        if self._sequence_token:
            kwargs = {'sequenceToken': self._sequence_token}

        response = self._client.put_log_events(
            logGroupName=self._group_name,
            logStreamName=self._stream_name,
            logEvents=[
                {'timestamp': timestamp, 'message': message}
                for timestamp, message in records_batch
            ],
            **kwargs)

        logger.debug(f'shipped {len(records_batch)} log records '
                     f'to {self.log_path}')

        self._sequence_token = response['nextSequenceToken']

    def _gather_log_records(self):
        records_size = 0
        records_batch = []

        while len(self):
            timestamp, message = self.popleft()

            if records_size + len(message) + 26 > 1048576 or \
                    len(records_batch) + 1 > 10000:
                try:
                    self._publish_records(records_batch)
                except self._client.exceptions.InvalidSequenceTokenException:
                    logger.exception(f'publishing failed for {self.log_path}')
                    self._sequence_token = self._get_sequence_token()
                    self.extend(records_batch)
                    records_batch = []
                    break
                except self._client.exceptions.BotoCoreError:
                    logger.exception(f'publishing failed for {self.log_path}')
                    self.extend(records_batch)
                    records_batch = []
                    break

                records_size = 0
                records_batch = []

            records_size += len(message) + 26
            records_batch.append((timestamp, message))

        if records_batch:
            try:
                self._publish_records(records_batch)
            except self._client.exceptions.InvalidSequenceTokenException as e:
                logger.exception(f'publishing failed for {self.log_path}')
                self._sequence_token = self._get_sequence_token()
                self.extend(records_batch)
            except self._client.exceptions.BotoCoreError:
                logger.exception(f'publishing failed for {self.log_path}')
                self.extend(records_batch)

        self._last_flush_timestamp = time.time()

    def flush(self, require_publish=False):
        next_ship_timestamp = self._last_flush_timestamp + self._flush_interval
        if require_publish or \
                next_ship_timestamp <= time.time() or \
                len(self) >= self._flush_threshold:

            if not self._publish_thread or not self._publish_thread.is_alive():

                if self._publish_thread is not None:
                    self._publish_thread.join()

                self._publish_thread = threading.Thread(
                    target=self._gather_log_records)
                self._publish_thread.start()


class WriterTaskType(enum.IntEnum):
    PUSH = 0
    FLUSH = 1
    REGISTER = 2
    UNREGISTER = 3


class CloudWatchLogWriter(threading.Thread):

    def __init__(self, queue, close_lock, *args, **kwargs):
        super(CloudWatchLogWriter, self).__init__(*args, **kwargs)
        self.daemon = True
        self._queue = queue
        self._close_lock = close_lock
        self._log_streams = {}

    def _execute_task(self, task, routing_key, task_data):
        if task == WriterTaskType.PUSH:
            self._log_streams[routing_key].append(task_data)

        elif task == WriterTaskType.FLUSH:
            self._log_streams[routing_key].flush()

        elif task == WriterTaskType.REGISTER:
            client = boto3.client(
                'logs',
                aws_access_key_id=task_data['aws_access_key_id'],
                aws_secret_access_key=task_data['aws_secret_access_key'],
                region_name=task_data['region_name'])

            self._log_streams[routing_key] = CloudWatchLogEventStorage(
                flush_interval=task_data['interval'],
                flush_threshold=task_data['threshold'],
                persistent=task_data['persistent'],
                client=client,
                group_name=task_data['group_name'],
                stream_name=task_data['stream_name'])

        elif task == WriterTaskType.UNREGISTER:
            self._log_streams[routing_key].flush()
            del self._log_streams[routing_key]

    def _consume_queue_tasks(self):
        consumed_tasks = 0
        while True:
            try:
                task_definition = self._queue.get(block=False)
            except (Empty, EOFError, KeyboardInterrupt, SystemExit):
                break
            consumed_tasks += 1
            self._execute_task(*self.deserialize_task(task_definition))
        logger.debug(f'consumed {consumed_tasks} tasks from the queue')
        return consumed_tasks

    def run(self):
        # Consumption capacity is the message ingestion rate
        consumption_capacity = 1000
        wait_time = 1.0

        try:
            while not self._close_lock.wait(wait_time):
                consumed_count = self._consume_queue_tasks()

                for log_stream in self._log_streams.values():
                    logger.debug('queue has %s log records in %s' % (
                        len(log_stream), log_stream.log_path))
                    log_stream.flush()

                # Dynamically adjustable timer dependant on task consumption
                if consumed_count > consumption_capacity:
                    consumption_capacity = consumed_count
                wait_time = consumption_capacity**(
                        -float(consumed_count) / consumption_capacity)
                logger.debug('consumer will wait %.4f' % wait_time)
                logger.debug(threading.get_ident())

        except (KeyboardInterrupt, SystemExit):
            pass

        finally:
            self._consume_queue_tasks()

            for log_stream in self._log_streams.values():
                log_stream.flush(require_publish=True)

            for log_stream in self._log_streams.values():
                log_stream.publisher.join()

    @staticmethod
    def serialize_task(task, routing_key, payload=None):
        if isinstance(routing_key, six.string_types):
            routing_key = routing_key.encode('utf-8')
        return struct.pack('!B10s', task, routing_key) + marshal.dumps(payload)

    @staticmethod
    def deserialize_task(data):
        task, routing_key = struct.unpack('!B10s', data[:11])
        return task, routing_key, marshal.loads(data[11:])


class Singleton(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class CloudWatchLogCollector:
    # Log collector must be a singleton to reduce necessary thread count
    # to one for any count of log handlers.

    def __init__(self):
        self._close_lock = threading.Event()
        self._queue = Queue()
        self._writer = CloudWatchLogWriter(self._queue, self._close_lock)
        self._writer.start()

        atexit.register(self.close)

    def register_log_stream(self, routing_key, group_name, stream_name,
                            interval, threshold, persistent, region_name,
                            aws_access_key_id, aws_secret_access_key):

        self._queue.put(self._writer.serialize_task(
            WriterTaskType.REGISTER, routing_key, {
                'group_name': group_name,
                'stream_name': stream_name,
                'interval': interval,
                'threshold': threshold,
                'persistent': persistent,
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'region_name': region_name,
            }
        ))

        return lambda timestamp, message: self._queue.put(
            self._writer.serialize_task(
                WriterTaskType.PUSH, routing_key.encode('utf-8'),
                (timestamp, message)))

    def unregister_log_stream(self, routing_key):
        self._queue.put(self._writer.serialize_task(
            WriterTaskType.UNREGISTER, routing_key))

    def flush(self, routing_key):
        self._queue.put(self._writer.serialize_task(
            WriterTaskType.FLUSH, routing_key))

    def close(self):
        self._close_lock.set()
        self._writer.join()


class CloudWatchLogHandler(logging.Handler):

    def __init__(self, group_name, stream_name, interval=10, threshold=1000,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 region_name=None, persistent=False, *args, **kwargs):
        super(CloudWatchLogHandler, self).__init__(*args, **kwargs)

        assert group_name and isinstance(group_name, six.string_types) and \
            re.match(r'[.\-_/#A-Za-z0-9]{1,512}', group_name), \
            'Log group name is invalid. Must match [.-_/#A-Za-z0-9]+ and ' \
            'from 1 to 512 character long.'

        assert stream_name and isinstance(stream_name, six.string_types) and \
            re.match(r'[^:*]{1,512}', stream_name), \
            'Log stream name is invalid. Must not include * or : and ' \
            'must be from 1 to 512 characters long'

        assert isinstance(interval, six.integer_types) and interval > 0, \
            'Interval must be greater than zero.'

        self._routing_key = hashlib.sha1(
            f'{group_name}/{stream_name}'.encode('utf-8')).hexdigest()[:10]
        self._collector = CloudWatchLogCollector()
        self._emit = self._collector.register_log_stream(
            routing_key=self._routing_key,
            group_name=group_name,
            stream_name=stream_name,
            interval=interval,
            threshold=threshold,
            persistent=persistent,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name)

    def emit(self, record):
        try:
            message = self.format(record)
            self._emit(int(time.time()*1000.0), message)
        except Exception:
            self.handleError(record)

    def flush(self):
        self._collector.flush(self._routing_key)

    def close(self):
        self._collector.unregister_log_stream(self._routing_key)
        super(CloudWatchLogHandler, self).close()
