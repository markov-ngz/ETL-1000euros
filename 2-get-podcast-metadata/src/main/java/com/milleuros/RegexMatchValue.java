package com.milleuros;


public class RegexMatchValue implements Comparable<RegexMatchValue>{
    public String value ; 
    public Integer start ; 
    public Integer end ;     
    
    public RegexMatchValue(String value, int start, int end){
        this.value = value ; 
        this.start = start ; 
        this.end = end ;
    }

    @Override
    public int compareTo(RegexMatchValue o){
        
        return this.start.compareTo(o.start) ;
    }



}
