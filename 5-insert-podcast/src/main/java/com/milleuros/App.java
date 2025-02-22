package com.milleuros;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.markovngz.rabbitmq.ConsumerRMQ;
import com.markovngz.Config;
import com.markovngz.podcast.Podcast;



public class App {
    public static void main(String[] args) throws Exception{

        // 0. Setup 
        Config config = new Config(new String[] {"RABBITMQ_HOST","CONSUME_QUEUE_NAME","DB_HOST","DB_PORT","DB_NAME","DB_USERNAME","DB_PASSWORD"}) ;   
        int dbPort = Integer.parseInt(config.get("DB_PORT")) ;
        PodcastDatabase dbHandler = new PodcastDatabase("postgresql",config.get("DB_HOST"), dbPort, config.get("DB_NAME"), config.get("DB_USERNAME"),config.get("DB_PASSWORD"));
        String rabbitMQHost = config.get("RABBITMQ_HOST") ;
        String consumingQueueName = config.get("CONSUME_QUEUE_NAME") ; 
        
        ObjectMapper objectMapper = new ObjectMapper();

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMQHost);
        Connection connection = connectionFactory.newConnection() ;
        ConsumerRMQ consumerRMQ = new ConsumerRMQ(connection) ; 

        // Consume message and load it into database 
        consumerRMQ.consume(consumingQueueName, false, 30, message -> {
            Podcast podcast = parseJson(message, Podcast.class, objectMapper) ; 

            dbHandler.singleLoading(podcast);

        });

        connection.close();
        if(dbHandler.isConnected()){
            dbHandler.disconnect();
        }


    }

    public static <T> T parseJson(String jsonMessage, Class<T> clazz, ObjectMapper objectMapper) {

        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }

    public static String getExample(){
        return """
        [ {
            "id" : 0,
            "urlPodcast" : "http://example.com",
            "podcastName" : null,
            "urlsMedia" : "http://example.media.com",
            "fileName" : null,
            "podcastDate" : "2024-11-30",
            "createdAt" : null,
            "updatedAt" : null,
            "podcast_available" : null
            }, {
            "id" : 0,
            "urlPodcast" : "http://example4.com",
            "podcastName" : null,
            "urlsMedia" : "http://example4.media.com",
            "fileName" : null,
            "podcastDate" : "2024-12-05",
            "createdAt" : null,
            "updatedAt" : null,
            "podcast_available" : null
            } ]       
        """ ;
    }
}
