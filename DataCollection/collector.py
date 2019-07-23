'''https://mail.python.org/pipermail/tutor/2005-October/042616.html'''
import threading as td
from queue import Queue, Empty
import sys
import psutil
from datetime import datetime
from db import Db
import time

'''
threads=1                       Int                 Amount of threads to be used
consumer_amount=50              Int                 Batch size to be passed to consumer functions
inserter_size=1e+8              Int                 Maximum memory usage of inserter in bytes
messages                        Dictionary          Messages to be displayed
maximum_memory                  Int                 % of max memory used. This uses memory used by all software.

Producer functions must return a list of lists.
Consumer functions must accept one parameter which is a list of size consumer_amount and return a list
Inserter functions must accept one parameter which is a list from consumer and return an int

Message options:
    loop_restart
    loop_start
    loop_update
    
    producer_start
    consumer_start
    inserter_start
    inserter_end
    
'''


class Collector(Db):
    # How many consumers and inserters there are
    t_consumer = 0
    t_inserter = 0

    # Queues
    q_cons = Queue()
    q_ins = Queue()

    # Keeping track of threads
    thread_list = []

    # Variables to be overridden
    ins_table = None
    cons_amount = None
    threads = None
    ins_size = 1e+8

    def __init__(self, maximum_memory=90, **kwargs):
        self.max_mem = maximum_memory   # Maximum memory usage
        super(Collector, self).__init__(**kwargs)

    #   Prepares workers for launch
    def start(self, multi=True):
        condition = self.condition()
        while condition:
            for t in self.thread_list:
                t.join()

            if multi:
                self._loop()
            else:
                self._loop_single()
            condition = self.condition()

        # Emptying Thread list for next loop
        self.thread_list = []
        if condition:
            self.messages('loop_restart')
            self.start()

        self.messages('starter_end')

    #   Launches workers
    def _loop(self):
        self.messages('loop_start')
        self._producer()

        while self.q_cons.qsize() + self.q_ins.qsize():

            #   Checking how many threads there are
            if self.t_consumer + self.t_inserter > self.threads:
                continue

            self.messages('loop_update',
                          thread_count=self.t_consumer + self.t_inserter,
                          producer_queue=self.q_cons.qsize(),
                          consumers=self.t_consumer,
                          insert_queue=self.q_ins.qsize(),
                          inserters=self.t_inserter,
                          threads=self.threads)

            if self.q_ins.qsize() > self.t_inserter:
                t = td.Thread(target=self._inserter)

            elif self.q_cons.qsize() > self.t_consumer:
                t = td.Thread(target=self._consumer)
            else:
                time.sleep(3)
                continue

            self.thread_list.append(t)
            try:
                t.start()
            except RuntimeError:
                pass

        try:
            self._inserter()
        except:
            pass
        self.q_cons.join()
        self.q_ins.join()

        # Waiting for inserters to finish
        # Consumers should have processed all the data since producer is not multi-threaded and provides all the data
        while self.t_inserter > 0:
            continue

    def _loop_single(self):
        self.messages('loop_start')
        self._producer()

        while self.q_cons.qsize() + self.q_ins.qsize():

            self.messages('loop_update',
                          thread_count=self.t_consumer + self.t_inserter,
                          producer_queue=self.q_cons.qsize(),
                          consumers=self.t_consumer,
                          insert_queue=self.q_ins.qsize(),
                          inserters=self.t_inserter,
                          threads=self.threads)

            if self.q_ins.qsize() > self.t_inserter:
                self._inserter()

            elif self.q_cons.qsize() > self.t_consumer:
                self._consumer()
            else:
                continue

        try:
            self._inserter()
        except:
            pass

        self.q_cons.join()
        self.q_ins.join()

        # Waiting for inserters to finish
        # Consumers should have processed all the data since producer is not multi-threaded and provides all the data
        while self.t_inserter > 0:
            continue

    #   Pulls data from local database and inserts them into queue
    #   Must return a list of data
    def _producer(self):
        self.messages('producer_start')

        data_list = self.producer()

        for data in data_list:
            self.q_cons.put(data)

    #   Pulls data from producer queue and further works with them through Spotify API
    #   Amount sets how many data should be passed to the consumer function
    def _consumer(self):
        self.messages('consumer_start')
        self.t_consumer += 1

        # Runs while there are tracks to be processed and memory is less than X
        while self.q_cons.qsize() > 0 and psutil.virtual_memory()[2] <= self.max_mem:
            data = []
            try:
                # Getting 50 ids or rest of queue
                for _ in range(self.cons_amount if self.q_cons.qsize() >= self.cons_amount else self.q_cons.qsize()):
                    data.append(self.q_cons.get_nowait())
                    self.q_cons.task_done()
            except Empty:
                break

            ret_data = self.consumer(data)

            for row in ret_data:
                self.q_ins.put(row)

        self.t_consumer -= 1

    #   Pulls data from consumer queue and prepares them for insert into local database
    def _inserter(self):
        self.messages('inserter_start')
        self.t_inserter += 1
        ins_len = []
        insert_list = []

        while sys.getsizeof(insert_list) < self.ins_size:
            try:
                data = self.q_ins.get_nowait()
            except Empty:
                break

            ins_len = self.inserter(data)

            self.q_ins.task_done()

        self.messages('inserter_end', insert_size=ins_len)
        self.t_inserter -= 1

    def _class_name(self):
        return self.__class__.__name__

    def messages(self, message, **kwargs):
        thread_count = str(kwargs.get('thread_count', ''))
        producer_queue = str(kwargs.get('producer_queue', ''))
        consumers = str(kwargs.get('consumers', ''))
        insert_queue = str(kwargs.get('insert_queue', ''))
        inserters = str(kwargs.get('inserters', ''))
        threads = str(kwargs.get('threads', ''))
        insert_size = str(kwargs.get('insert_size', ''))

        class_name = self._class_name()

        messages = {'loop_restart': '[' + class_name + ']'
                                    ' Found more items for update. Restarting loop at ' + str(datetime.now()),
                    'loop_start': '[' + class_name + '] Starting collection at ' + str(datetime.now()),
                    'loop_update': '[' + class_name + '] Threads: ' + thread_count
                                                              + ' Consumer Queue: ' + producer_queue
                                                              + ' Consumers: ' + consumers
                                                              + ' Insert Queue: ' + insert_queue
                                                              + ' Inserters: ' + inserters
                                                              + ' Max threads: ' + threads,
                    'producer_start': '[' + class_name + '] Producer thread started at ' + str(datetime.now())
                                      + '. Please wait while it gathers data.',
                    'consumer_start': '[' + class_name + '] Consumer thread started at ' + str(datetime.now()),
                    'inserter_start': '[' + class_name + '] Inserter thread started at ' + str(datetime.now()),
                    'inserter_end': '[' + class_name + '][+] ' + insert_size
                                    + ' records inserted at ' + str(datetime.now()),
                    'starter_end': '[' + class_name + '][+] Finished!'}

        print(messages[message])

    # Bellow methods have to be overridden by child class
    def condition(self):
        pass

    def producer(self):
        return []

    def consumer(self, *args):
        return []

    def inserter(self, *args):
        pass