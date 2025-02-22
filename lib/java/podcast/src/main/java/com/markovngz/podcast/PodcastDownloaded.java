package com.markovngz.podcast;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class PodcastDownloaded extends Podcast {
    String mp3 ; // base64 encoded bytes of the mp3

    public void downloadMp3(){
        HttpClient client = HttpClient.newHttpClient() ; 

        HttpRequest request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(this.getUrlsMedia()))
        .build();

        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            
            this.mp3 = Base64.getEncoder().encodeToString(response.body()) ;
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public byte[] getMp3Bytes(){
        return Base64.getDecoder().decode(mp3) ; 
    }

}