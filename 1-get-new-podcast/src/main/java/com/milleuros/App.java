package com.milleuros;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.markovngz.database.DBHandler;
import com.markovngz.Config;
import com.markovngz.rabbitmq.PublisherRMQ;
import com.markovngz.http.HTTPHandler;
import com.markovngz.http.WebScrapper;

public class App {
    

    /*
     * Get a list of new podcasts 
     */
    public static void main(String[] args) throws Exception{

        // 0. Basic configuration 
        Config config = new Config(new String[] {"RABBITMQ_HOST","EXCHANGE_NAME","PUBLISH_QUEUE_NAMES","DB_HOST","DB_PORT","DB_NAME","DB_USERNAME","DB_PASSWORD"}) ;  

        String rabbitMQHost = config.get("RABBITMQ_HOST") ;
        String fanoutExchangeName = config.get("EXCHANGE_NAME") ; 
        String[] publishingQueueNames = config.get("PUBLISH_QUEUE_NAMES").split(",") ; 

        HTTPHandler httpHandler = new HTTPHandler() ; 

        WebScrapper scrapper = new WebScrapper(httpHandler) ; 

        ObjectMapper objectMapper = new ObjectMapper();

        // 1. Connect to RabbitMQ cluster & Database 

        // RabbitMq Connection 
        ConnectionFactory connectionFactory = new ConnectionFactory() ;
        connectionFactory.setHost(rabbitMQHost);
        
        Connection connection = connectionFactory.newConnection() ;
        PublisherRMQ publisherRMQ = new PublisherRMQ(connection, true);
        publisherRMQ.bindQueuesToFanoutExchange(fanoutExchangeName, publishingQueueNames, true,true);
        publisherRMQ.addAsyncConfirmListener();

        // Database Connection
        int dbPort = Integer.parseInt(config.get("DB_PORT")) ;
        DBHandler dbHandler = new DBHandler("postgresql",config.get("DB_HOST"),dbPort, config.get("DB_NAME"), config.get("DB_USERNAME"),config.get("DB_PASSWORD"));


        // 2. Get the new podcasts 
        PodcastHandler podcastHandler = new PodcastHandler(dbHandler, scrapper, objectMapper) ;
        List<String> newUrlPodcasts = podcastHandler.getNewPodcasts() ; 

        // 3. Write into a topic the new podcasts
        for(String newUrlPodcast : newUrlPodcasts){
            publisherRMQ.publishMessage(fanoutExchangeName,newUrlPodcast) ;
        }
        publisherRMQ.waitAsyncPublishingConfirmation(20);

        connection.close() ; 
    }

}