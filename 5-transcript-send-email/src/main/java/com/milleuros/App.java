package com.milleuros;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.markovngz.rabbitmq.ConsumerRMQ;
import com.markovngz.utils.Config;
import com.markovngz.podcast.PodcastTranscript;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception{

        // 0. Setup
        Config config = new Config(new String[] {"RABBITMQ_HOST","CONSUME_QUEUE_NAME","SMTP_HOST","SMTP_PORT","SMTP_SENDER","SMTP_PASSWORD","SMTP_RECEIVERS"}) ; 
        
        String rabbitMQHost = config.get("RABBITMQ_HOST") ;
        String consumingQueueName = config.get("CONSUME_QUEUE_NAME")  ; 
        ObjectMapper objectMapper = new ObjectMapper();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMQHost);

        Connection connection = connectionFactory.newConnection() ; 
        ConsumerRMQ consumerRMQ = new ConsumerRMQ(connection) ; 

        EmailSender emailSender = new EmailSender(config.get("SMTP_SENDER"), config.get("SMTP_PASSWORD"), true, true, config.get("SMTP_HOST"), config.get("SMTP_PORT"), null) ; 

        
        // 1. For each message, send an email with the podcast transcription

        consumerRMQ.consume(consumingQueueName, false, 42, message -> {

            // 1.1 Parse message
            PodcastTranscript podcastTranscript =  parseJson(message, PodcastTranscript.class, objectMapper) ;

            // 1.2 Send as email 
            emailSender.sendEmail(config.get("SMTP_SENDER"), config.get("SMTP_RECEIVERS"), podcastTranscript.getPodcastName(),podcastTranscript.getTranscript()) ; 

        });

        connection.close();
    }

    public static <T> T parseJson(String jsonMessage, Class<T> clazz, ObjectMapper objectMapper) {
        

        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message", e);
        }
    }

}
