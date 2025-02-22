package com.markovngz.http;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;

import java.net.URI;

public class HTTPHandler {

    private String apiKey ; 
    private HttpClient client = HttpClient.newHttpClient() ; 

    public HTTPHandler() {}

    public void setApiKey(String apiKey){
        this.apiKey = apiKey ; 
    }
    
    /*
     * Send an unauthenticated request
     */
    public HttpResponse<String> sendGET(String url){
        HttpRequest request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(url))
        .build();
        
        try {
            HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
            return response ;
        } catch (Exception e) {
            throw new Error(e);
        }
    }


    /*
     *  Send a post request with the api key as bearer token  
     */
    public HttpResponse<String> sendPOST(String url, String payload){
        HttpRequest request = HttpRequest.newBuilder()
        .POST(BodyPublishers.ofString(payload))
        .header("Authorization", String.format("Bearer %s",this.apiKey))
        .header("Content-Type", "application/json")
        .header("x-wait-for-model", "true") // Specific Huggingface api
        .uri(URI.create(url))
        .build(); 

        try {
            // genericType that holds a string 
            HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
            return response ; 
        }catch(Exception e){
            throw new Error(e);
        }  
    }
}