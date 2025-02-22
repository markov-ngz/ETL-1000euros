package com.milleuros;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.AMQP ; 

import com.fasterxml.jackson.databind.ObjectMapper;

import com.markovngz.rabbitmq.PublisherRMQ;
import com.markovngz.rabbitmq.ConsumerRMQ;
import com.markovngz.Config;
import com.markovngz.podcast.Podcast;
import com.markovngz.podcast.PodcastDownloaded;
import com.markovngz.hdfs.HDFSHandler;

public class App {
    

    public static void main(String[] args) throws Exception {

        // 1. Base Setup 

        // environment variables 
        Config config = new Config(new String[] {"RABBITMQ_HOST","CONSUME_QUEUE_NAME","EXCHANGE_NAME","PUBLISH_QUEUE_NAMES","HDFS_HOST","HDFS_PODCAST_FOLDER"}) ; 
        String rabbitMQHost = config.get("RABBITMQ_HOST") ;
        String consumingQueueName = config.get("CONSUME_QUEUE_NAME")  ; 
        String fanoutExchangeName = config.get("EXCHANGE_NAME")  ; 
        String[] publishingQueueNames = config.get("PUBLISH_QUEUE_NAMES").split(",")  ;
        String hdfsHost =  config.get("HDFS_HOST")   ; 
        String podcastHDFSFolder = config.get("HDFS_PODCAST_FOLDER")  ;

        ObjectMapper objectMapper = new ObjectMapper();

        // rabbitmq connection
        Connection connection = connectionFactory.newConnection() ; 
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMQHost);
        PublisherRMQ publisherRMQ = new PublisherRMQ(connection, true) ; 
        publisherRMQ.bindQueuesToFanoutExchange(fanoutExchangeName, publishingQueueNames,true,true);
        ConsumerRMQ consumerRMQ = new ConsumerRMQ(connection) ; 

        // hdfs connection 
        HDFSHandler hdfsHandler = new HDFSHandler(hdfsHost);
        hdfsHandler.connect();

        // 
        consumerRMQ.consume(consumingQueueName, false, 30, message -> {

            // 1. Deserialize the payload
            PodcastDownloaded podcastDownloaded = parseJson(message, PodcastDownloaded.class, objectMapper) ;

            // 2. Upload Audio to HDFS
            String filePath = podcastHDFSFolder + "/" + podcastDownloaded.getFileName() ; 
            hdfsHandler.writeBytes(podcastDownloaded.getMp3Bytes(), filePath);

            // 3. Publish podcast to queue in order to insert it into database
            // misconception where the child class mp3 attribute are still seraialized even if the object is "only" a podcast object
            // hence the podcast is manually created from the child object 
            Podcast podcast = getUnderlyingPodcast(podcastDownloaded) ; 
            

            String jsonPodcast  = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(podcast);
 
            System.out.println(jsonPodcast);
            publisherRMQ.publishMessage(fanoutExchangeName, jsonPodcast);
        
        });

        hdfsHandler.close();
        connection.close() ; 

        
    }

    public static Podcast getUnderlyingPodcast(PodcastDownloaded podcastDownloaded){
        Podcast podcast = new Podcast() ; 

        podcast.setUrlPodcast(podcastDownloaded.getUrlPodcast());
        podcast.setPodcastName(podcastDownloaded.getPodcastName());
        podcast.setUrlsMedia(podcastDownloaded.getUrlsMedia());
        podcast.setFileName(podcastDownloaded.getFileName());
        podcast.setPodcastDate(podcastDownloaded.getPodcastDate());
        podcast.setCreatedAt(podcastDownloaded.getCreatedAt()) ;
        podcast.setUpdatedAt(podcastDownloaded.getUpdatedAt());
        podcast.setPodcastAvailable(true);

        return podcast ; 
    }   

    public static <T> T parseJson(String jsonMessage, Class<T> clazz, ObjectMapper objectMapper) {
        
        
        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }

    public static void writeAudio(byte[] audioBytes, String fileName){
        
        try {
            // Initialize HDFS operations
            HDFSHandler hdfs = new HDFSHandler("hdfs://localhost:9000");
            hdfs.connect();

            hdfs.writeBytes(audioBytes, fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

