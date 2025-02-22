package com.milleuros;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.markovngz.podcast.NewPodcastDto ;
import com.markovngz.database.DBHandler;
import com.markovngz.http.WebScrapper;

public class PodcastHandler {

    private final DBHandler dbHandler ;
    private final WebScrapper webScrapper ; 
    private final ObjectMapper objectMapper ; 

    private String baseUrl = "https://www.radiofrance.fr/franceinter/podcasts/le-jeu-des-1000" ; 

    public PodcastHandler(
        DBHandler dbHandler,
        WebScrapper webScrapper,
        ObjectMapper objectMapper
    ){
        this.dbHandler = dbHandler ; 
        this.webScrapper = webScrapper ; 
        this.objectMapper = objectMapper ; 
    }

    /*
     * Get new podcasts based on given existing 
     */
    public String getLastPodcast()throws SQLException{

        String lastPodcast = null ; 
        
        String statement = "SELECT url_podcast FROM podcasts ORDER BY podcast_date DESC LIMIT 1"; 

        try {
            dbHandler.connect();

            ResultSet resultSet = dbHandler.executeQuery(statement);
            
            while (resultSet.next()) {
                lastPodcast = resultSet.getString("url_podcast") ; 
            }
            
            return lastPodcast ; 

        }catch(Exception e){
            throw e ;
        } finally {
            dbHandler.disconnect();
        }

        
    }

    public List<String> getNewPodcasts()throws Exception {

        // 1. Get last podcast
        String lastPodcast = getLastPodcast() ; 

        // 2. Get new podcast until the last podcast is encountered
        return getNewPodcastsURL(lastPodcast) ; 

    }

    public  List<String> getNewPodcastsURL( String lastEmissionUrl) throws Exception{
        

        // parameter to iter over the pages 
        String requestParameter = "?p=%s" ;
        // add the url  
        String loopUrl = baseUrl + requestParameter ; 

        // new podcasts
        List<String> newEmissions =  new ArrayList<String>() ; 

        // handle first podcast separately as there is redirection and HTTP class does not handle this (yet?)
        NewPodcastDto lastEmissions = getPodcasts(baseUrl, lastEmissionUrl) ; 
        // add them to container
        newEmissions.addAll(lastEmissions.podcasts) ;
        boolean end = lastEmissions.getEnd() ; 
        

        for(int i = 2; end == false; i ++){
            
            // format url 
            String pageUrl = String.format(loopUrl, String.valueOf(i)) ; 

            // Get the pages podcast
            NewPodcastDto lastEmissionsPaginated = getPodcasts(pageUrl, lastEmissionUrl) ; 
            
            end = lastEmissionsPaginated.getEnd() ;
            // System.out.println(String.valueOf(lastEmissions.podcasts));
            
            newEmissions.addAll(lastEmissionsPaginated.podcasts) ;

        }

        
        return newEmissions;
    }

    public  NewPodcastDto getPodcasts(String urlPage, String lastEmissionUrl) throws Exception{


        // html node attributes 
        String node = "script" ; 
        String attribute = "type" ; 
        String attributeValue = "application/ld+json" ; 


        // get HTML 
        String html = webScrapper.extractHTML(urlPage);

        // load as a Document to navigate through the nodes 
        Document doc = webScrapper.getHTMLDocument(html) ;

        // get the target element value which is json deserialized
        String data = webScrapper.getFirstElementwithAttributeValue(doc, node, attribute, attributeValue) ;

        //create JsonNode
        JsonNode rootNode = objectMapper.readTree(data);

        // get the item list , no condition : the node must exist 
        JsonNode listItems = rootNode.get("@graph").get(1).get("itemListElement") ; 

        // container to append the podcasts's url
        List<String> l = new  ArrayList<String>() ;
        boolean endEncountered = false ;

        /// navigate until the index is out of range 
        for(int i=0 ;listItems.has(i) ; i++ ){

            // get the item 
            JsonNode item = listItems.get(i) ; 
            // does the item has url field
            if(item.has("url")){
                // extract the url 
                String s  = item.get("url").asText() ; 

                // as the items are in descending time order,
                //  it means the first ones are the newest hence once the url match all beyond that index are already present in the database 

                if(s.equals(lastEmissionUrl) | s == lastEmissionUrl){ 
                    endEncountered = true  ; 
                    break ; 
                }
                // add it 
                l.add(s) ; 
            }
        }
 

        return  new NewPodcastDto(endEncountered, l) ;

    }
}
