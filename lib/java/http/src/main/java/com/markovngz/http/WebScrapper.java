package com.markovngz.http;

import java.net.http.HttpResponse;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document ;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements; 
import com.markovngz.http.HTTPHandler;

public class WebScrapper {

    private final HTTPHandler httpHandler ; 

    public WebScrapper(HTTPHandler httpHandler){
        this.httpHandler = httpHandler ; 
    }

    public String extractHTML(String url) throws Exception{
        HttpResponse<String> response = this.httpHandler.sendGET(url) ; 

        if (response.statusCode() != 200 ){
            throw  new Exception(String.format("Invalid status code received. Expected 200 , got %s",String.valueOf(response.statusCode()))) ; 
        }

        return response.body() ; 
    }

    public Document getHTMLDocument(String html){
        return Jsoup.parse(html) ; 
    }

    public String getFirstElementwithAttributeValue(Document doc, String node, String attribute, String attributeValue){

        Elements elements = doc.getElementsByTag(node) ; 

        for (Element element : elements) {
            if(element.hasAttr(attribute)){

                if(element.attr(attribute).equals(attributeValue) ){

                    return element.data();     
                }
            }
        }

        return null ; 
    }
}
