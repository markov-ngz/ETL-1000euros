package com.milleuros;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.markovngz.podcast.Podcast;
import com.markovngz.http.HTTPHandler;



public class MetadataExtract {
    
    public List<Podcast> podcasts ; 
    
    private final HTTPHandler httpHandler ; 

    private ObjectMapper objectMapper = new ObjectMapper() ; 

    public MetadataExtract(List<String> podcastsUrl, HTTPHandler httpHandler){
        
        this.initPodcasts(podcastsUrl);
        
        this.httpHandler = httpHandler ; 
    }

    public void initPodcasts(List<String> podcastsUrl){

        List<Podcast> l = new ArrayList<Podcast>() ; 

        for(String s: podcastsUrl ){
            Podcast p = new Podcast() ;
            p.setUrlPodcast(s); 
            p.setPodcastName(s.substring(s.lastIndexOf("/") +1 ));
            l.add(p) ; 
        }
        
        this.podcasts = l ; 
       
    }
    
    public Map<String,Podcast> getPodcastswithMediaUrls(){

        Map<String,Podcast> podcastMap = new HashMap<String,Podcast>() ; 

        for(Podcast podcast : this.podcasts){

            // is the podcast's media url is already obtained , do not process 
            if(podcastMap.containsKey(podcast.getUrlPodcast())){
                continue ;
            }else{
                // this.podcasts.stream().map(p -> p.getUrl_podcast()).toList() 
                // create the URL
                String targetUrl = createTargetUrl(podcast.getUrlPodcast()) ; 
                
                // get the raw content
                String content = getContent(targetUrl) ;
                
                // extract media urls inside the response  
                List<RegexMatchValue> mediaUrls = extractMediaUrlsLocations(content);
                
                // extract the podcasts inside the response 
                NavigableMap<RegexMatchValue,Podcast> podcastRegex = extractPodcastsIndexes(content) ; 

                // match the podcasts to the media urls
                List<Podcast> listPodcasts =  matchPodcastToMediaUrl(podcastRegex, mediaUrls) ;
                
                // append to response 
                for(Podcast podcastWithMediaUrl : listPodcasts){podcastMap.put(podcastWithMediaUrl.getUrlPodcast(), podcastWithMediaUrl) ; }
            }

        }

        return podcastMap ; 


    }

    public String serializePodcasts(List<Podcast> podcasts) throws JsonProcessingException{
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(podcasts) ; 
    }

    public String createTargetUrl(String podcastUrl){
        String append = "__data.json?x-sveltekit-invalidated=111" ; 
        if(! podcastUrl.endsWith("/")){
            podcastUrl+= "/" ;
        }
        return podcastUrl + append ; 
    }

    public String getContent(String targetUrl){
        HttpResponse<String> response = this.httpHandler.sendGET(targetUrl) ; 
        return response.body() ; 
    }

    public Matcher matchRegexMediaUrl(String s ){
        String expression = "https://media\\.radiofrance-podcast\\.net[^\\s,]*\\.mp3" ;
        Pattern pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE) ; 
        Matcher matcher = pattern.matcher(s) ; 

        return matcher ; 
    }

    public List<RegexMatchValue>  extractMediaUrlsLocations(String s){
        
        List<RegexMatchValue> l = new ArrayList<RegexMatchValue>() ; 

        Matcher matcher = this.matchRegexMediaUrl(s);

        while(matcher.find()){
            RegexMatchValue regexMatchValue = new RegexMatchValue(matcher.group(), matcher.start() , matcher.end()) ; 
            l.add(regexMatchValue) ; 
        }

        return l ; 
    }

    public NavigableMap<RegexMatchValue,Podcast> extractPodcastsIndexes( String s){
        
        NavigableMap<RegexMatchValue,Podcast> m = new TreeMap<RegexMatchValue, Podcast>(); 

        for(Podcast podcast: this.podcasts){

            String namePodcast  = podcast.getPodcastName() ; 
            
            int startIndex = s.indexOf(namePodcast) ; 
            int endIndex = -1 ; 

            if (startIndex != -1){
                endIndex = startIndex + namePodcast.length() ; 
                RegexMatchValue regexMatchValue = new RegexMatchValue(namePodcast,startIndex, endIndex) ;
                m.put(regexMatchValue, podcast ); 
            }else{
                continue ; // the podcast name was not in the response string hence do not add it
            }

        }
        

        return m ; 
        
    }

    public int getNextIndex(Map.Entry<RegexMatchValue,Podcast> entry, NavigableMap<RegexMatchValue,Podcast> podcastRegex, Map.Entry<RegexMatchValue,Podcast> lastEntry){
        if(! entry.equals(lastEntry)){
            Map.Entry<RegexMatchValue,Podcast> next = podcastRegex.higherEntry(entry.getKey()) ;
            return next.getKey().start ; 
        }else{
            return 1000000 ; // absurd value to be sure that all the media urls after the last are destined to the last entry
        } 
    }


    public Date extractDatefromFileName(String podcastFilename){

        String[] parts = podcastFilename.split("-") ;
            
        if (parts.length >= 2) {

            
            String fullDatePart = parts[1] ; 

            String[] dateParts = fullDatePart.split("\\.") ; 
            
            if(dateParts.length == 3){
                String formattedDate = String.format("%s-%s-%s", dateParts[2],dateParts[1],dateParts[0]) ; 

                Date date = Date.valueOf(formattedDate) ;
                
                return date ; 
            
            }else{
                System.out.println("<date parse err>");
                // Log error
            }
        }
        return null ; 
    }   

    public String extractPodcastFileName(String  mediaUrl){

        String[] split =  mediaUrl.split("/") ; 
        if(split.length > 0){
            return split[split.length -1] ;  // last item is the filename
        }else{
            return null ; 
        }
    }

    public List<Podcast>  matchPodcastToMediaUrl(NavigableMap<RegexMatchValue,Podcast> podcastRegex , List<RegexMatchValue> mediaRegex){
        
        List<Podcast> l = new ArrayList<Podcast>() ; 

        Map.Entry<RegexMatchValue,Podcast> lastEntry  =podcastRegex.lastEntry() ; 

        int nextIndex = 0 ; 
        int mediaIndex = 0 ; 
        for(Map.Entry<RegexMatchValue,Podcast>  m : podcastRegex.entrySet()){

            // get next Index
            nextIndex = getNextIndex(m, podcastRegex, lastEntry) ; 

            List<String> mediaUrls =  new ArrayList<String>() ; 

            while(mediaIndex < mediaRegex.size()){
                if(nextIndex > mediaRegex.get(mediaIndex).start){
                    mediaUrls.add(mediaRegex.get(mediaIndex).value) ;
                }else{
                    break ; 
                }
                mediaIndex ++ ; 
            }

            // if it is empty do not append to value 
            if(mediaUrls.isEmpty()){
                continue ;
            }else{
                String mediaUrl = mediaUrls.get(0)  ; 
                
                m.getValue().setUrlsMedia(mediaUrl);
                
                String podcastFileName = extractPodcastFileName(mediaUrl) ;

                m.getValue().setFileName(podcastFileName);

                Date podcastDate = extractDatefromFileName(podcastFileName) ; 

                m.getValue().setPodcastDate(podcastDate);
            }

            l.add(m.getValue()) ;
            
        }
        return l ;
    }


}
