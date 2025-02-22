import pika 
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
class Publisher():

    def __init__(self, connection:BlockingConnection)->None:
        self.channel : BlockingChannel = connection.channel()
     

    def bind_queues(self,exchange:str,queues:list[str], exchange_type="fanout", create_queues:bool=True)->None:
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        for queue in queues : 
            if create_queues : self.channel.queue_declare(queue=queue)
            self.channel.queue_bind(exchange=exchange,queue=queue)
    
    def add_confirm_publish(self):
        self.channel.confirm_delivery()

    def publish(self,exchange:str, body : str|bytes, routing_key:str="")->None:
        self.channel.basic_publish(exchange=exchange,routing_key=routing_key,body=body) 