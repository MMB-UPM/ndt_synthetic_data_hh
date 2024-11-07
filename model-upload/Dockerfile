FROM python:3.8.10

ENV CATALOG_URL="http://localhost:9200"

ENV CATALOG_INDEX="models"

ENV MODELS_PATH="./models/rev3/"

ENV MODEL="random_forest_train_ceos2_eth4_29_final_10estimators.joblib"

RUN apt-get update -y

RUN python3 -m pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install --upgrade setuptools wheel pip-tools

WORKDIR /usr/app/src

COPY files/requirements.txt ./

RUN python3 -m pip install -r requirements.txt

RUN mkdir models

#COPY files/models/* ./models/

COPY files/model_upload.py ./

CMD python3 ./model_upload.py --url=$CATALOG_URL --index=$CATALOG_INDEX --path=$MODELS_PATH --filename=$MODEL
