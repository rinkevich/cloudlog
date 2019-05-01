
import atexit
import logging
import re
import signal
import time
from queue import Empty, Queue
from threading import Event, Thread

import boto3
import six

logger = logging.getLogger(__name__)


class CloudWatchLogWriter(Thread):

    def __init__(self, streams, close_signal):
        super(CloudWatchLogWriter, self).__init__()
        self.daemon = True

        self._streams = streams
        self._close_signal = close_signal

    @staticmethod
    def save_log_events(log_events, group_name, stream_name,
                        client, sequence_token):
        kwargs = {
            'logGroupName': group_name,
            'logStreamName': stream_name,
            'logEvents': log_events
        }
        if sequence_token:
            kwargs['sequenceToken'] = sequence_token
        response = client.put_log_events(**kwargs)
        return response['nextSequenceToken']

    def pull_logs_events(self, queue):
        log_events = []
        payload_size = 0
        payload_pushed = False

        while True:
            try:
                timestamp, message = queue.get(block=False)
            except Empty:
                break

            if payload_size + len(message) + 26 > 1048576 or \
                    len(log_events) == 10000:
                sequence_token = self.save_log_events(
                    log_events,
                    group_name=queue.group_name,
                    stream_name=queue.stream_name,
                    client=queue.client,
                    sequence_token=queue.sequence_token
                )
                queue.sequence_token = sequence_token
                payload_pushed = True

                log_events = []
                payload_size = 0

            log_events.append({'timestamp': timestamp, 'message': message})
            payload_size += len(message) + 26

        if len(log_events):
            sequence_token = self.save_log_events(
                log_events,
                group_name=queue.group_name,
                stream_name=queue.stream_name,
                client=queue.client,
                sequence_token=queue.sequence_token
            )
            queue.sequence_token = sequence_token
            payload_pushed = True

        if payload_pushed:
            queue.last_flush = int(time.time())

        if queue.force_flush.is_set():
            queue.force_flush.clear()

    def run(self):
        while not self._close_signal.wait(timeout=1):
            for queue in self._streams.values():
                if queue.can_pull_events:
                    self.pull_logs_events(queue)

        for queue in self._streams.values():
            self.pull_logs_events(queue)


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


class CloudWatchLogEntryQueue(Queue):

    def __init__(self, group_name, stream_name, interval=10,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 region_name=None, *args, **kwargs):
        super(CloudWatchLogEntryQueue, self).__init__(*args, **kwargs)

        self.group_name = group_name
        self.stream_name = stream_name

        self.interval = interval

        self.client = boto3.client(
            'logs',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name)
        self.sequence_token = None
        self.describe_or_create_log_streams()

        self.last_flush = int(time.time())
        self.force_flush = Event()

    def describe_or_create_log_streams(self):
        client = self.client

        try:
            client.create_log_group(logGroupName=self.group_name)
        except client.exceptions.ResourceAlreadyExistsException:
            pass

        try:
            client.create_log_stream(
                logGroupName=self.group_name,
                logStreamName=self.stream_name)
        except client.exceptions.ResourceAlreadyExistsException:
            response = client.describe_log_streams(
                logGroupName=self.group_name,
                logStreamNamePrefix=self.stream_name,
                orderBy='LogStreamName', descending=False, limit=1)
            self.sequence_token = response['logStreams'][0]\
                .get('uploadSequenceToken')

    @property
    def can_pull_events(self):
        if self.force_flush.is_set():
            return True

        if int(time.time()) >= self.last_flush + self.interval and self.qsize():
            return True

        elif self.qsize() > 10000:
            return True
        return False


class CloudWatchLogCollector(metaclass=Singleton):
    # Log collector is implemented as a singleton, in result reduces branching
    # when multiple log handlers are used. This prevents from creating
    # a thread for every log handler.

    def __init__(self):
        self._streams = {}

        self._close_signal = Event()
        self._writer = CloudWatchLogWriter(self._streams, self._close_signal)
        self._writer.start()

        # If application exits uncleanly, will try to flush log data.
        signal.signal(signal.SIGHUP, self.close)
        signal.signal(signal.SIGTERM, self.close)
        signal.signal(signal.SIGINT, self.close)
        atexit.register(self.close)

    def attach(self, group_name, stream_name, interval,
               aws_access_key_id, aws_secret_access_key, region_name):

        queue = CloudWatchLogEntryQueue(
            group_name, stream_name, interval,
            aws_access_key_id, aws_secret_access_key, region_name)

        self._streams[f'{group_name}/{stream_name}'] = queue
        return queue

    def deattach(self, group_name, stream_name):
        stream = self._streams[f'{group_name}/{stream_name}']
        stream.force_flush.set()
        stream.force_flush.wait()
        del self._streams[f'{group_name}/{stream_name}']

    def flush(self, group_name, stream_name):
        stream = self._streams[f'{group_name}/{stream_name}']
        stream.force_flush.set()

    def close(self):
        self._close_signal.set()
        self._writer.join(timeout=5)


class CloudWatchLogHandler(logging.Handler):

    def __init__(self, group_name, stream_name, interval=10,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 region_name=None, *args, **kwargs):
        super(CloudWatchLogHandler, self).__init__(*args, **kwargs)

        assert group_name and isinstance(group_name, six.string_types) and \
            re.match(r'[.\-_/#A-Za-z0-9]+', group_name), \
            'Log group name is invalid. Must match [.-_/#A-Za-z0-9]+'

        assert stream_name and isinstance(stream_name, six.string_types) and \
            re.match(r'[^:*]{1,512}', stream_name), \
            'Stream name is invalid. Must not include * or :. Acceptable ' \
            'length is 1 to 512 bytes.'

        assert interval > 0, \
            'Interval must be greater than zero.'

        self._group_name = group_name
        self._stream_name = stream_name

        self._collector = CloudWatchLogCollector()
        self._queue = self._collector.attach(
            group_name=group_name,
            stream_name=stream_name,
            interval=interval,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name)

    def emit(self, record):
        try:
            message = self.format(record)
            self._queue.put((int(time.time()*1000.0), message))
        except Exception:
            self.handleError(record)

    def flush(self):
        self._collector.flush(self._group_name, self._stream_name)

    def close(self):
        self._collector.deattach(self._group_name, self._stream_name)
        super(CloudWatchLogHandler, self).close()
