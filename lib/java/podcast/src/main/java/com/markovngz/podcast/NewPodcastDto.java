package com.markovngz.podcast;
import java.util.List;

import lombok.Data;

/**
 * Dto to get the both information : 
 * 1. Is there an emission among the one scrapped that was already present in based ? 
 * 2. Add the new emissions ( not exisitng in database) 
 */
@Data
public class NewPodcastDto {
    public boolean end ; 
    public List<String> podcasts ; 

    public NewPodcastDto(boolean end ,List<String> podcasts){
        this.end = end ; 
        this.podcasts = podcasts ; 
    }

    public boolean getEnd(){
        return this.end ; 
    }
    
}