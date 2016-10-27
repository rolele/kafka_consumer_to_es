from kafka import KafkaConsumer
from sys import argv
import traceback
import treq
from twisted.internet import defer
import json
import requests
from twisted.internet import reactor, defer
from twisted.internet import defer, task, threads
import six
from twisted.internet.task import cooperate
from twisted.internet.task import LoopingCall
import optparse, sys

def post(data):
    d = treq.post('http://es:9200/properties/property/', data)
    d.addCallbacks(lambda x: print(x), lambda x: print("error %s " % x))

import time
from twisted.internet import reactor, threads
from twisted.internet.task import LoopingCall
import datetime

def blockingApiCall(arg):
    try:
        record = six.advance_iterator(arg)
        post(record.value)
    except StopIteration as e:
        print(e)
        end = datetime.datetime.now()
        print(end - start)
        print(i)
        print("finish")
        reactor.stop()


def nonblockingCall(arg):
    print(arg)

def printResult(result):
    print(result)

def finish():
    reactor.stop()
from kafka import TopicPartition
start = datetime.datetime.now()
s = start.ctime()
print(s)
consumer = KafkaConsumer(bootstrap_servers="kafka:9092", group_id=s, auto_offset_reset='earliest')
#consumer.assign([TopicPartition('kafkapipeline', 1)])
consumer.subscribe(['kafkapipeline'])
print("start loop")
LoopingCall(blockingApiCall, consumer).start(.001)


# d = threads.deferToThread(blockingApiCall, "Goose")
# d.addCallback(printResult)
# LoopingCall(blockingApiCall, "Duck").start(.1)
# reactor.callLater(10, finish)
reactor.run()

#curl -XPUT 'http://es:9200/twitter/tweet/1' -d '{
#    "user" : "kimchy",
#    "post_date" : "2009-11-15T14:12:12",
#    "message" : "trying out Elasticsearch"
#}'

#curl -XDELETE 'http://es:9200/properties/'
