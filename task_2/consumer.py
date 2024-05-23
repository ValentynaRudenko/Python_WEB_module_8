import pika
import json
from models import Contacts
import connect

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost',
                              port=5672,
                              credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='htask2_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def send_email(email):
    pass


def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    print(f" [x] Received {message}")

    _id = message["contact"]
    email = message["email"]
    contact = Contacts.objects(id=_id)

    send_email(email)
    contact.update(set__message_sent=True)

    print(f" [x] Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='htask2_queue', on_message_callback=callback)


if __name__ == '__main__':
    channel.start_consuming()
