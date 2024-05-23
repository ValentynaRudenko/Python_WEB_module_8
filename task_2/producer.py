import pika
from datetime import datetime
import sys
import json
import faker
from models import Contacts
import connect

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost',
                              port=5672,
                              credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='task_mock', exchange_type='direct')
channel.queue_declare(queue='htask2_queue', durable=True)
channel.queue_bind(exchange='task_mock', queue='htask2_queue')

fake_data = faker.Faker()


def contact_generator():
    for i in range(fake_data.random_int(3, 7)):
        Contacts(fullname=str(fake_data.name()),
                 email=str(fake_data.email()),
                 ).save()
    contacts = Contacts.objects()
    return contacts


def main():
    for contact in contact_generator():
        message = {
            "contact": str(contact.id),
            "email": str(contact.email),
            "date": datetime.now().isoformat()
        }

        channel.basic_publish(
            exchange='task_mock',
            routing_key='htask2_queue',
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(" [x] Sent %r" % message)

    connection.close()

    
if __name__ == '__main__':
    main()
