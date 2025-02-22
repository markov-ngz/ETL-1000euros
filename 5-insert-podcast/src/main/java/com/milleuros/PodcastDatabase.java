package com.milleuros;

import java.sql.SQLException;
import java.util.List;

import org.postgresql.util.PSQLException;

import com.markovngz.database.DBHandler;
import com.markovngz.podcast.Podcast;

/**
 * Custom DBClass to Load podcasts
 */
public class PodcastDatabase extends DBHandler {

    public PodcastDatabase(String driver , String host, int port, String database, String user, String password){
        super(driver, host, port, database, user, password) ;
    }

    /*
     * Batch load per 
     * 
     */
    public void batchLoading(List<Podcast> podcasts, int batch_size)throws SQLException, PSQLException{

        int maxSlots = Math.floorDiv(podcasts.size(), batch_size) +1;

        for(int i=0; i < maxSlots ; i++){
            
            int startIndex = i*batch_size ;
            int endIndex = (i+1)*batch_size; 
           
            // handle end batch size 
            if(endIndex > podcasts.size()){ endIndex = podcasts.size()  ; } 

            List<Podcast> batch = podcasts.subList(startIndex, endIndex) ; 

            String podcastsSerialized = serializePodcasts(batch) ; 

            String statement = writeMergePodcastStatement(podcastsSerialized) ; 

            int success = executeUpdate(statement) ;

            System.out.println(success);

        }

        
    }

    public void singleLoading(Podcast podcast) throws SQLException{
        String podcastSerialized = serializePodcast(podcast) ;

        String statement =  writeMergePodcastStatement(podcastSerialized) ; 
        System.out.println(statement);
        int success = executeUpdate(statement) ; 
    }

    public String serializePodcast(Podcast podcast){
        String baseStr = "('%s','%s','%s','%s','%s'::date,%s)" ;
        return  String.format(baseStr,
                                podcast.getPodcastName(), 
                                podcast.getUrlPodcast(), 
                                podcast.getUrlsMedia(), 
                                podcast.getFileName(),
                                podcast.getPodcastDate().toString(),
                                true
        );
    }

    public String serializePodcasts(List<Podcast> podcasts){

        return String.join(",\n\t", podcasts.stream().map(p -> serializePodcast(p)).toList() ) ;
    }

    public String writeMergePodcastStatement(String podcastsStr){
        String baseStr = """
        MERGE INTO podcasts AS target 
        USING 
            (VALUES 
                %s
            ) 
            AS source(name, url_podcast,urls_media,file_name,podcast_date,podcast_available) ON target.url_podcast = source.url_podcast
        WHEN MATCHED THEN UPDATE SET name=source.name , urls_media = source.urls_media ,podcast_date=source.podcast_date, file_name=source.file_name,podcast_available=source.podcast_available ,updated_at=CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (name, url_podcast,urls_media, file_name,podcast_date,podcast_available,created_at,updated_at) VALUES (source.name,source.url_podcast, source.urls_media,source.file_name ,source.podcast_date,source.podcast_available,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ;       
        """;

        return String.format(baseStr, podcastsStr) ; 
     }

}

// MERGE INTO podcasts AS target 
// USING 
//     (VALUES 
//         ('https://example.com','https://media.example.com/example.mp3','1999-10-10'::date),
//         ('https://example2.com','https://2media.example.com/example.mp3','2145-01-01'::date),
//         ('https://example3.com','https://3media.example.com/example.mp3','1822-01-01'::date)
//     ) 
//     AS source(url_podcast,urls_media,podcast_date) ON target.url_podcast = source.url_podcast
// WHEN MATCHED THEN UPDATE SET urls_media = source.urls_media ,podcast_date=source.podcast_date,updated_at=CURRENT_TIMESTAMP
// WHEN NOT MATCHED THEN INSERT (url_podcast,urls_media,podcast_date,created_at,updated_at) VALUES (source.url_podcast, source.urls_media, source.podcast_date,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ;