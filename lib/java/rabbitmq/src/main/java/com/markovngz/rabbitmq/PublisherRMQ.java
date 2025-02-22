package com.markovngz.rabbitmq;

import java.io.IOException;
import java.sql.Time;
import java.time.Duration;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;


public class PublisherRMQ {

    private final Connection connection ;
    private Channel channel ; 
    private ConcurrentNavigableMap<Long, String> outstandingConfirms ;
    private boolean confirmSelect ; 
    private boolean asyncConfirmation = false; 

    public PublisherRMQ(Connection connection, boolean confirmSelect)throws IOException{
        this.connection = connection ; 
        this.confirmSelect = confirmSelect ; 
        buildChannel(this.confirmSelect);
    }

    /*
     * Instantiate a channel
     */
    private void buildChannel(boolean confirmSelect) throws IOException{
        this.channel = this.connection.createChannel() ;
        if(confirmSelect){
            this.channel.confirmSelect() ; 
        }
    }

    /*
     * Declare a fanout exchange and bind it the specified queues 
     */
    public void bindQueuesToFanoutExchange(String exchange, String[] queues,boolean durable ,boolean declareQueues) throws IOException{
        // declare fanout exchange
        channel.exchangeDeclare(exchange, "fanout", false, false, null);
        // bind queues
        for(String queue : queues){
            
            if(declareQueues){channel.queueDeclare(queue,true,false,false,null) ;}
            
            channel.queueBind(queue, exchange, "") ; // as it is fanout the routingKey is ignored 
        }
    }

    /*
     * Add confirm listener to channel
     * 
     * @source https://github.com/rabbitmq/rabbitmq-tutorials/blob/main/java/PublisherConfirms.java
     */
    public void addAsyncConfirmListener(){
        this.outstandingConfirms = new ConcurrentSkipListMap<>(); 

        ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
            if (multiple) {
                ConcurrentNavigableMap<Long, String> confirmed = this.outstandingConfirms.headMap(
                        sequenceNumber, true
                );
                confirmed.clear();
            } else {
                this.outstandingConfirms.remove(sequenceNumber);
            }
        };

        // Callback for nack messages 
        ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
            // get the message 
            String body = this.outstandingConfirms.get(sequenceNumber);
            
            System.err.format(
                    "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                    body, sequenceNumber, multiple
            );

            ackCallback.handle(sequenceNumber, multiple); // call the ack one to remove the message from the list 
        };

        this.channel.addConfirmListener(ackCallback,nackCallback );
        this.asyncConfirmation = true ; 
    }

    /*
     * Publish a message, if the confirm select mode is on, it will wait for 5 sec for broker's confirmation
     */
    public void publishMessage(String exchangeName,String payload)throws IOException,InterruptedException,TimeoutException{
        if(asyncConfirmation){this.outstandingConfirms.put(this.channel.getNextPublishSeqNo(), payload) ;}
        channel.basicPublish(exchangeName, "", null, payload.getBytes());
        if(confirmSelect & !asyncConfirmation){
                channel.waitForConfirmsOrDie(5_000) ;
        }
    }

    /*
     * Wait for all messages published before the specified timeout
     */
    public void waitAsyncPublishingConfirmation(long secondTimeout) throws InterruptedException{
        if (!waitUntil(Duration.ofSeconds(secondTimeout), () -> outstandingConfirms.isEmpty())) {
            throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
        }
    }

    private boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }

}