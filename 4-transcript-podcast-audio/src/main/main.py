import pika 
from utility import Config
from podcast import Podcast
import json 
from cortex import Transcriptor
from rabbitmq import  Publisher , Consumer

def main():

    config = Config(["EXCHANGE_NAME","CONSUME_QUEUE_NAME","PUBLISH_QUEUE_NAMES","MODEL_PATH","PROCESSSOR_PATH","DEVICE","RABBITMQ_HOST"])

    LANGUAGE="french"

    # 0. Setup connection to RabbitMQ broker    & Transcription object
    transcriptor = Transcriptor(config.MODEL_PATH,config.PROCESSSOR_PATH,language=LANGUAGE,device=config.DEVICE)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config.RABBITMQ_HOST))
    
    publisher = Publisher(connection)
    publisher.bind_queues(config.EXCHANGE_NAME,config.PUBLISH_QUEUE_NAMES)



    def process_message(message:bytes):

        # 1. Map podcast json to object 
        podcast = json.loads(message,object_hook=lambda x : Podcast(**x))

        # 2. Transcript the audio 
        podcast.transcription = transcriptor.transcript(podcast.get_mp3_audio())
        
        # 3. Serialize to JSON 
        dict_podcast = podcast.__dict__
        dict_podcast.pop("mp3") # remove unwanted key

        # 4. Publish message to ending queue
        publisher.publish(config.EXCHANGE_NAME,dict_podcast.__str__()) 


    consumer = Consumer(connection)
    consumer.consume(config.CONSUME_QUEUE_NAME,process_message)

    connection.close()



if __name__ == "__main__":
    main()