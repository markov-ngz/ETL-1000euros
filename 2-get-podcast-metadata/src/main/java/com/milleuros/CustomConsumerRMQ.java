package com.milleuros;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.markovngz.rabbitmq.ThrowingConsumer;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class CustomConsumerRMQ extends com.markovngz.rabbitmq.ConsumerRMQ{
    
    public CustomConsumerRMQ(Connection connection) throws Exception{
        super(connection);
    }

    public void consumeDelivery(String queueName,boolean autoAck,long secondTimeout ,ThrowingConsumer<Delivery> consumer) throws IOException, InterruptedException{
        
        CountDownLatch countDownLatch = this.buildCountDownLatch(queueName) ; 

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                consumer.accept(delivery) ; 
                // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                countDownLatch.countDown();
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                
            }
        };
        
        this.channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });

        countDownLatch.await(secondTimeout, TimeUnit.SECONDS) ; 
    }
}
