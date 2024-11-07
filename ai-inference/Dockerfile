FROM python:3.8.10

ENV KAFKA_URL="localhost:9094"

ENV CATALOG_URL="localhost:9200"

ENV CONSUMER_TOPIC="inference_data"

ENV PRODUCER_TOPIC="inference_probs"

ENV CONSUMER_CLIENT_ID="ai-inference-consumer"

ENV PRODUCER_CLIENT_ID="ai-inference-producer"

ENV CONSUMER_GROUP_ID="ai-inference"

ENV CATALOG_MODELS_INDEX="models"

ENV LOAD_MODEL="None"

RUN apt-get update -y

RUN python3 -m pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install --upgrade setuptools wheel pip-tools

WORKDIR /usr/app/src

COPY files/requirements.txt ./

RUN python3 -m pip install -r requirements.txt

COPY files/ai_inference.py ./

COPY files/ai_inference_multiprocessing.py ./

COPY files/ai_inference_buffer.py ./

ENV TZ="Europe/Madrid"

CMD python3 ./ai_inference_buffer.py --kafka_url=$KAFKA_URL --catalog_url=$CATALOG_URL --consumer_topic=$CONSUMER_TOPIC --producer_topic=$PRODUCER_TOPIC --consumer_client_id=$CONSUMER_CLIENT_ID --producer_client_id=$PRODUCER_CLIENT_ID --consumer_group_id=$CONSUMER_GROUP_ID --catalog_models_index=$CATALOG_MODELS_INDEX --load_model=$LOAD_MODEL
