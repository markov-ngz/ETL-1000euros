package com.milleuros;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;

import com.markovngz.rabbitmq.PublisherRMQ;
import com.markovngz.rabbitmq.ConsumerRMQ;
import com.markovngz.http.HTTPHandler;
import com.markovngz.Config;
import com.markovngz.podcast.Podcast;
import com.markovngz.podcast.PodcastDownloaded;


public class App {
    public static void main(String[] args)throws Exception {

        // 1. RabbitMQ base Setup 
        Config config = new Config(new String[] {"RABBITMQ_HOST","CONSUME_QUEUE_NAME","EXCHANGE_NAME","PUBLISH_QUEUE_NAMES"}) ; 
        
        String rabbitMQHost = config.get("RABBITMQ_HOST") ;
        String consumingQueueName = config.get("CONSUME_QUEUE_NAME")  ; 
        String fanoutExchangeName = config.get("EXCHANGE_NAME")  ; 
        String[] publishingQueueNames = config.get("PUBLISH_QUEUE_NAMES").split(",")  ;

        ObjectMapper objectMapper = new ObjectMapper();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMQHost);

        Connection connection = connectionFactory.newConnection() ; 

        PublisherRMQ publisherRMQ = new PublisherRMQ(connection, true) ; 
        publisherRMQ.bindQueuesToFanoutExchange(fanoutExchangeName, publishingQueueNames,true,true);
        publisherRMQ.addAsyncConfirmListener();

        ConsumerRMQ consumerRMQ = new ConsumerRMQ(connection) ; 

        // 2. Consume the queue and for each message 

        consumerRMQ.consume(consumingQueueName, false, 42, message -> {

            // 2.1 parse the content 
            PodcastDownloaded podcast =  parseJson(message, PodcastDownloaded.class, objectMapper) ;

            // 2.2 Download the podcast as base64 encoded String 
            podcast.downloadMp3();

            // 2.3 Write as json 
            String jsonEncoded = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(podcast);

            // 2.4 wirte into the following exchange
            publisherRMQ.publishMessage(fanoutExchangeName, jsonEncoded);
        });

        publisherRMQ.waitAsyncPublishingConfirmation(30);
        connection.close();
        
    }

    public static <T> T parseJson(String jsonMessage, Class<T> clazz, ObjectMapper objectMapper) {
        

        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }

    public static String getContent(){
        return """
        [ {
            "id" : 0,
            "urlPodcast" : "https://www.radiofrance.fr/franceinter/podcasts/le-jeu-des-1-000/le-jeu-des-1000-du-mercredi-23-octobre-2024-5869075",
            "podcastName" : "le-jeu-des-1-000/le-jeu-des-1000-du-mercredi-23-octobre-2024-5869075",
            "urlsMedia" : "https://media.radiofrance-podcast.net/podcast09/10206-23.10.2024-ITEMA_23901850-2024F4004S0297-22.mp3",
            "fileName" : "10206-23.10.2024-ITEMA_23901850-2024F4004S0297-22.mp3",
            "podcastDate" : "2024-10-12",
            "createdAt" : null,
            "updatedAt" : null,
            "podcast_available" : null
            }, {
            "id" : 0,
            "urlPodcast" : "https://www.radiofrance.fr/franceinter/podcasts/le-jeu-des-1-000/le-jeu-des-1000-du-mardi-24-septembre-2024-9558050",
            "podcastName" : "le-jeu-des-1-000/le-jeu-des-1000-du-mardi-24-septembre-2024-9558050",
            "urlsMedia" : "https://media.radiofrance-podcast.net/podcast09/10206-24.09.2024-ITEMA_23870744-2024F4004S0268-22.mp3",
            "fileName" : "10206-24.09.2024-ITEMA_23870744-2024F4004S0268-22.mp3",
            "podcastDate" : "2024-09-24",
            "createdAt" : null,
            "updatedAt" : null,
            "podcast_available" : null
            } ]  
        """;
    }
}
