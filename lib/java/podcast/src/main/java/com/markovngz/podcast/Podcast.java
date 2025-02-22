package com.markovngz.podcast;

import java.sql.Date;
import java.sql.Timestamp;

import lombok.Data;

@Data
public class Podcast {
    private  int id ;  // UUID as string
    private String urlPodcast;
    private String podcastName ; 
    private String urlsMedia ;
    private String fileName ;
    private Date podcastDate ;
    private Timestamp createdAt ;
    private Timestamp updatedAt;
    private boolean podcastAvailable ; 

    public Podcast(){
    }
}