import pika 
import time 

class Consumer():
    def __init__(self, connection:pika.BlockingConnection):
        self.channel = connection.channel()

    def consume(self,queue:str,func : callable ,auto_ack:bool=False, )-> None:

        def callback(ch, method, properties, body):
            # print(body)
            func(body)

        self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=auto_ack)
        self.channel.start_consuming()
   