CREATE TABLE public.podcasts
(
    id serial,
    name VARCHAR(255),
    url_podcast VARCHAR(255),
    urls_media VARCHAR(512),
    file_name VARCHAR(255),
    podcast_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    podcast_available BOOLEAN,
    PRIMARY KEY (id)
);

-- Insert a new row with a UUID
INSERT INTO podcasts (name, url_podcast, urls_media, file_name, podcast_date, created_at, updated_at, podcast_available)
VALUES (
    'le-jeu-des-1000-du-vendredi-20-decembre-2024-8260271',
    'https://www.radiofrance.fr/franceinter/podcasts/le-jeu-des-1-000/le-jeu-des-1000-du-vendredi-20-decembre-2024-8260271', 
    'https://media.radiofrance-podcast.net/podcast09/10206-20.12.2024-ITEMA_23971388-2024F4004S0355-22.mp3|https://media.radiofrance-podcast.net/podcast09/22898-20.12.2024-ITEMA_23971866-2024F4004S0355-21.mp3', 
    '10206-20.12.2024-ITEMA_23971388-2024F4004S0355-22.mp3', 
    '2024-12-20', 
    CURRENT_TIMESTAMP, 
    CURRENT_TIMESTAMP, 
    TRUE);

