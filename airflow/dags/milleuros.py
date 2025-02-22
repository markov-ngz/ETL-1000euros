from airflow.decorators import task, dag 
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime
from airflow.models import Variable
from docker.types import Mount

@dag(start_date=datetime(2024,1,1), schedule_interval="@weekly", catchup=False,tags=["milleuros"],)
def milleuros_pipeline():

    RABBITMQ_HOST=Variable.get("RABBITMQ_HOST")
    HDFS_URL=Variable.get("HDFS_URL")
    DB_HOST=Variable.get("DB_HOST")
    DB_PORT=Variable.get("DB_PORT")
    DB_NAME=Variable.get("DB_NAME")
    DB_USERNAME=Variable.get("DB_USERNAME")
    DB_PASSWORD=Variable.get("DB_PASSWORD")

    DOCKER_NETWORK = Variable.get("DOCKER_NETWORK")
    DOCKER_SOCKET_MOUNT="unix://var/run/docker.sock"
    DOCKER_USERNAME=Variable.get("DOCKER_USERNAME")


    # 1. Get the new podcasts URLs
    task = "get_new_podcasts"
    get_new_podcasts = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
            "RABBITMQ_HOST":RABBITMQ_HOST,
            "EXCHANGE_NAME":"new-podcast",
            "PUBLISH_QUEUE_NAMES":"podcast-urls",
            "DB_HOST":DB_HOST,
            "DB_PORT":DB_PORT,
            "DB_NAME":DB_NAME,
            "DB_USERNAME":DB_USERNAME,
            "DB_PASSWORD":DB_PASSWORD
            },
    )


    # 2. Get additional information about the podcast
    task = "get_podcast_metadata"
    get_podcast_metadata = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
             "RABBITMQ_HOST":RABBITMQ_HOST,
             "CONSUME_QUEUE_NAME":"podcast-urls",
             "EXCHANGE_NAME":"podcast-audio-saved",
             "PUBLISH_QUEUE_NAMES":"podcast-metadata",
            },
    )

    # 3. Download the audio
    task = "download_podcast_audio" 
    download_podcast_audio = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
            "RABBITMQ_HOST":RABBITMQ_HOST,
            "CONSUME_QUEUE_NAME":"podcast-metadata",
            "EXCHANGE_NAME":"audio-mp3",
            "PUBLISH_QUEUE_NAMES":"podcast-mp3,transcription-mp3"
            },
    )

    # 4.A Upload the audio the HDFS
    task = "upload_podcast_audio" 
    upload_podcast_audio = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
           "RABBITMQ_HOST":RABBITMQ_HOST,
            "CONSUME_QUEUE_NAME":"podcast-mp3",
            "EXCHANGE_NAME":"podcast-audio-saved",
            "PUBLISH_QUEUE_NAMES":"podcast-insert",
            "HDFS_HOST":HDFS_URL,
            "HDFS_PODCAST_FOLDER":"/milleuros",
            },
    )

    # 5.A Insert the podcast object into a database
    task ="insert_podcast"
    insert_podcast =DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
            "RABBITMQ_HOST":RABBITMQ_HOST,
            "CONSUME_QUEUE_NAME":"podcast-insert",
            "DB_HOST":DB_HOST,
            "DB_PORT":DB_PORT,
            "DB_NAME":DB_NAME,
            "DB_USERNAME":DB_USERNAME,
            "DB_PASSWORD":DB_PASSWORD

        },
    )

    # 4.B Transcript the audio
    task ="transcript_podcast_audio"
    transcript_podcast_audio = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
            "RABBITMQ_HOST":RABBITMQ_HOST,
            "EXCHANGE_NAME":"audio-transcript",
            "CONSUME_QUEUE_NAME":"transcription-mp3",
            "PUBLISH_QUEUE_NAMES":"transcription-podcast",
            "MODEL_PATH":"../../../../ai_resources/whisper-large-v3-turbo",
            "PROCESSSOR_PATH":"../../../../ai_resources/processor-whisper-large-v3-turbo",
            "DEVICE":"cpu",
            },
    )

    # 5.B Send the transcription by email 
    task ="transcript_send_email"
    transcript_send_email = DockerOperator(
        task_id=task,
        image="{0}/IMAGE_{1}".format(DOCKER_USERNAME,Variable.get("IMAGE_"+task)),
        container_name=task,
        docker_url=DOCKER_SOCKET_MOUNT, # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode=DOCKER_NETWORK, # name of container's running network
        environment={
            },
    )

    get_podcast_metadata << get_new_podcasts
    download_podcast_audio << get_podcast_metadata
    upload_podcast_audio << download_podcast_audio
    insert_podcast << upload_podcast_audio
    transcript_podcast_audio << download_podcast_audio
    transcript_send_email << transcript_podcast_audio

milleuros_pipeline()