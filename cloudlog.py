import atexit
import logging
import threading
import time
import uuid
from abc import abstractmethod
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from socket import gethostname

import boto3

__all__ = ('CentralizedLogHandler', 'CloudWatchLogHandler')


class CentralizedLogHandler(QueueHandler):
    """
    A queue handler to centralize multiple worker logging.

    https://docs.python.org/3/library/logging.handlers.html#queuehandler
    """
    __slots__ = "queue", "_listener"

    def __init__(self, handlers, queue=None, respect_handler_level=False):
        """
        Initialize queued log handler.

        :param list of logging.Handler handlers: Logging handlers
        :param queue.Queue queue: transport queue
        :param bool respect_handler_level: respect handler levels
        """
        self.queue = queue or Queue()
        self._listener = QueueListener(
            self.queue, *handlers,
            respect_handler_level=respect_handler_level)
        self._listener.start()
        atexit.register(lambda: self._listener.stop)
        super().__init__(self.queue)


class TimedBufferingHandler(logging.Handler):
    """
    A handler class which buffers logging records in memory. Record
    buffer flush is made when the buffer size exceeds the capacity or
    wait time for buffer records ends.

    """
    __slots__ = "capacity", "interval", "buffer", "timestamp", \
                "_exit_lock", "_thread"

    def __init__(self, capacity, interval, *args, **kwargs):
        """
        Initialize the handler with the buffer size and sender interval.

        :param int capacity: Buffer size
        :param int interval: Sender interval
        """
        logging.Handler.__init__(self, *args, **kwargs)
        super().__init__(*args, **kwargs)
        self.capacity = capacity
        self.interval = interval
        self.buffer = []
        self.timestamp = time.time()

        self._exit_lock = threading.Event()
        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()
        atexit.register(lambda: self._exit_lock.set() or self._thread.join())

    def run(self):
        """
        Daemon message drainer.

        """
        wait_time = self.interval

        try:
            while not self._exit_lock.wait(timeout=wait_time):
                if self.should_drain():
                    drained_capacity = self.drain(self.buffer[:self.capacity])
                    self.buffer = self.buffer[drained_capacity:]
                    self.timestamp = time.time()
        finally:
            self.flush()

    def should_drain(self):
        """
        Determine if the handler must flush its buffer.

        Returns true if the buffer is up to capacity or time interval
        ended. This method can be overridden to implement custom
        flushing strategies.
        """
        return (len(self.buffer) >= self.capacity) or \
               (time.time() - self.interval > self.timestamp)

    def emit(self, record):
        """
        Emit a record.

        Append the record to the buffer. No locks acquired or released
        to have an ability to append messages event when buffer is in
        progress of draining messages.
        """
        self.buffer.append(record)

    def flush(self):
        """
        Flush entire buffer.

        """
        drained_capacity = self.drain(self.buffer[:])
        self.buffer = self.buffer[drained_capacity:]

    def close(self):
        """
        Close the handler.

        This version just flushes and chains to the parent class" close().
        """
        try:
            self.flush()
        finally:
            logging.Handler.close(self)

    @abstractmethod
    def drain(self, buffer):
        """
        Try to drain buffer as much as possible and return how many
        records had been saved.

        :param list of logging.LogRecord buffer: LogRecord buffer
        :return: LogRecords processed
        :rtype: int
        """
        raise NotImplemented


class CloudWatchLogHandler(TimedBufferingHandler):
    """
    Cloudwatch logging handler.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#cloudwatchlogs

    """
    __slots__ = (
        "group_name", "stream_name", "aws_access_key_id",
        "aws_secret_access_key", "aws_session_token", "region_name",
        "client", "sequence_token",)

    def __init__(self, group_name, stream_name=None, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None,
                 region_name=None, *args, **kwargs):
        """
        Initialize CloudWatch logging handler

        :param str group_name: Group name
        :param str stream_name: Stream name
        """
        super().__init__(*args, **kwargs)
        self.group_name = group_name
        self.stream_name = stream_name or "%s-%s" % (
            gethostname().replace(".", "-"), uuid.uuid4().hex)

        self.client = boto3.client(
            "logs",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name, )
        self.sequence_token = self._get_sequence_token()

    def _get_sequence_token(self):
        try:
            response = self.client.describe_log_streams(
                logGroupName=self.group_name,
                logStreamNamePrefix=self.stream_name,
                orderBy="LogStreamName",
                descending=False,
                limit=1)
            return response["logStreams"][0].get("uploadSequenceToken")
        except self.client.exceptions.ResourceNotFoundException:
            self.client.create_log_stream(
                logGroupName=self.group_name,
                logStreamName=self.stream_name)
            return None

    def _put_log_events(self, records_batch):
        response = self.client.put_log_events(
            logGroupName=self.group_name,
            logStreamName=self.stream_name,
            logEvents=[
                {"timestamp": timestamp, "message": message}
                for timestamp, message in records_batch
            ],
            sequenceToken=self.sequence_token)
        self.sequence_token = response["nextSequenceToken"]

    def drain(self, buffer):
        """
        Drain buffer to CloudWatch stream.

        :param list of logging.LogRecord buffer: LogRecord buffer
        :return: successfully sent records
        :rtype: int
        """
        payload_size = 0
        message_batch = []
        sent_records = 0

        for record in buffer:
            message = self.format(record)
            timestamp = int(record.created)

            # If any of request payload restrictions occur, submit payload
            if payload_size + len(message) + 26 > 1048576 or \
                    len(message_batch) + 1 > 10000:
                try:
                    self._put_log_events(message_batch)
                except self.client.exceptions.InvalidSequenceTokenException:
                    self.sequence_token = self._get_sequence_token()
                    return sent_records
                sent_records += len(message_batch)
                message_batch = []
                payload_size = 0

            message_batch.append((timestamp, message))
            payload_size += len(message) + 26

        try:
            self._put_log_events(message_batch)
        except self.client.exceptions.InvalidSequenceTokenException:
            self.sequence_token = self._get_sequence_token()
            return sent_records
        return sent_records + len(message_batch)
