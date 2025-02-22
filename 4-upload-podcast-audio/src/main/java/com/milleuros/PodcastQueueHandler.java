package com.milleuros;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP ; 

import com.markovngz.hdfs.HDFSHandler;
import com.markovngz.podcast.PodcastDownloaded;
import com.markovngz.podcast.Podcast;


public class PodcastQueueHandler {

    private  ConnectionFactory connectionFactory ; 

    private ObjectMapper objectMapper ; 
    public Connection connection ; 
    public Channel channel ; 
    
    private final String podcastHDFSFolder ;
    
    private final HDFSHandler hdfs ; 

    public PodcastQueueHandler(String rabbitMQHost,String hdfsHost , String podcastHDFSFolder){
        this.podcastHDFSFolder = podcastHDFSFolder ; 

        this.connectionFactory = new ConnectionFactory();
        this.connectionFactory.setHost(rabbitMQHost);

        this.objectMapper = new ObjectMapper();

        this.hdfs = new HDFSHandler(hdfsHost);
    }

    /*
     * Connect to HDFS & RabbitMQ cluster
     * 
     */
    public void connect() {
        try {
            this.connection = this.connectionFactory.newConnection() ; 
            this.channel = this.connection.createChannel();
            this.hdfs.connect();
        } catch (Exception e) {
            e.printStackTrace();   
        }   
    }

    public void disconnect() {
        if(this.connection.isOpen()){
            try {
                this.connection.close();
                this.hdfs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * Get messages from a given queue and map it to the given object
     * 
     */
    public void uploadPodcastHDFS(String queueName, Consumer<String> consumer) throws Exception{

        // is a connection already opened ? 
        if(! this.connection.isOpen()){
            this.connect();
        }

        Channel publishingChannel = this.connection.createChannel() ; 
        String publishingQueueName = "podcast-insert" ; 
        publishingChannel.queueDeclare(publishingQueueName, false, false, false, null);
        publishingChannel.confirmSelect() ; 

        CountDownLatch countDownLatch = getNumberMessageToProcess(channel, queueName) ; 
        
        // set autoAck to false 
        boolean autoAck = false ; 

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String jsonPayload = new String(delivery.getBody(), "UTF-8");
            try {
                // 1. Deserialize Json message
                PodcastDownloaded podcastDownloaded = parseJson(jsonPayload, PodcastDownloaded.class)  ;

                // 2. Upload Audio to HDFS
                // writeAudio(podcastDownloaded);
                
                // 3. Publish podcast to queue 
                Podcast podcast = (Podcast) podcastDownloaded ; // get underlying podcast from podcastDownloaded object
                
                System.out.println(podcast.getFileName()); 
                System.out.println(podcast.getPodcastDate()); 
                // publish
                publishingChannel.basicPublish("", publishingQueueName, null, null);
                // wait for confirmation synchronously
                publishingChannel.waitForConfirmsOrDie(5_000) ;

                // 4. Ack the message 
                countDownLatch.countDown();
                
                // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }catch(Exception e ){
                e.printStackTrace(); 
            } finally {
              System.out.println(" [x] Done");
              
            }
          };
          
          try {
            channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> { });
            countDownLatch.await(30, TimeUnit.SECONDS) ; 

            // CountDownLatch latch = new CountDownLatch(messageCount);

          } catch (Exception e) {
            e.printStackTrace();
          }
       
    }

    public CountDownLatch getNumberMessageToProcess(Channel channel , String queueName) {
        // Get the current queue size (message count)
        try {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(queueName);
            int messageCount = response.getMessageCount();
            return new CountDownLatch(messageCount)  ; 
        } catch (Exception e) {
           e.printStackTrace();
           return null ; 
        }

    }

    public String buildFullPath(String fileName){
        return this.podcastHDFSFolder + "/" + fileName ; 
    }

    public void writeAudio(PodcastDownloaded podcastDownloaded) throws Exception{

        byte[] audioBytes = podcastDownloaded.getMp3Bytes() ; 
        String fullPath = this.buildFullPath( podcastDownloaded.getFileName()) ; 
        

        if(audioBytes != null){

            hdfs.writeBytes(audioBytes, fullPath);
        }
    }

    public <T> T parseJson(String jsonMessage, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }
}
