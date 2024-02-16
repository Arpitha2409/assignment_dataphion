from confluent_kafka import Producer
from random import randint
import time
import random, string, radar


p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

c = 0

while c < 10:
    c += 1

    rand_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))

    data = {
        "EventName": ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        "EventType": ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        "EventValue": randint(10, 100),
        # "EventTimestamp": radar.random_datetime(),
        "EventPageSource": ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        "EventPageURL": ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        "ComponentID": randint(100, 1000),
        "UserID": randint(1, 10)
        # "EventDate": radar.random_datetime()
    }

    print(data)

    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('new_topic', str(data).encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

