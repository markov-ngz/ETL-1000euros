package com.markovngz.rabbitmq;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class ConsumerRMQ {
    
    protected Connection connection ;
    protected Channel channel ; 

    public ConsumerRMQ(Connection connection)throws IOException{
        this.connection = connection ; 
        this.channel = this.connection.createChannel() ;
    }

    public CountDownLatch buildCountDownLatch(String queueName)throws IOException {
        // Get the current queue size (message count)
        AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(queueName);
        int messageCount = response.getMessageCount();
        return new CountDownLatch(messageCount)  ; 
    }

    public void consume(String queueName,boolean autoAck,long secondTimeout ,ThrowingConsumer<String> consumer) throws IOException, InterruptedException{
        
        CountDownLatch countDownLatch = buildCountDownLatch(queueName) ; 

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
            try {

                consumer.accept(message) ; 
                // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                countDownLatch.countDown();
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                System.out.println("Done");
            }
        };
        
        channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });

        countDownLatch.await(secondTimeout, TimeUnit.SECONDS) ; 
    }

    public void ack(Delivery delivery) throws IOException{
        this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }

    public void nack(Delivery delivery) throws IOException{
        this.channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
    }

}