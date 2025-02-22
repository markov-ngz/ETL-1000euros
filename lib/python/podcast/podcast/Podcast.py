import datetime
import json 
import base64

class Podcast():
    id : int
    urlPodcast : str
    podcastName : str  
    urlsMedia : str 
    fileName : str 
    podcastDate : datetime.date
    createdAt : datetime.date
    updatedAt : datetime.date
    podcastAvailable : bool 
    mp3 : str # base64encoding of the audio 
    transcription : str 

    def __init__(self,
                 id,
                 url_podcast,
                 podcast_name,
                 urls_media,
                 fileName,
                 podcastDate,
                 createdAt,
                 updated_At,
                 podcastAvailable):
        self.id = id 
        self.urlPodcast = url_podcast
        self.podcastName = podcast_name
        self.urlsMedia = urls_media 
        self.fileName = fileName 
        self.podcastDate = podcastDate 
        self.createdAt = createdAt 
        self.updatedAt = updated_At
        self.podcastAvailable = podcastAvailable

    def get_mp3_audio(self)->bytes:
        return base64.b64decode(self.mp3) 