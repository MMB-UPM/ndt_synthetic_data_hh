from base64 import b64decode
from typing import Tuple, List, Any, Optional
import logging
import os
import requests
from kafka import KafkaConsumer, KafkaProducer
from sys import stdout, exit
import time
from collections import namedtuple
import signal
import argparse
import pickle
import pandas as pd
from joblib import load
import numpy as np
from datetime import datetime


# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

# namedtuple for storing model information
Model = namedtuple("Model", ["id", "filename", "metadata", "local_path"])
ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic"])
ProducerConfig = namedtuple("ProducerConfig", ["client_id", "topic"])


BUFFER_SIZE = 300
TIMEOUT = 0.500

class AIInference:
    def __init__(self, kafka_url: str, catalog_url: str, models_index: str, producer_client_id: str,
                 consumer_client_id: str, consumer_group_id: str, consumer_topic: str, producer_topic: str,
                 load_model: str = None):
        LOGGER.info("Initializing AI Inference...")

        self.broker = kafka_url
        LOGGER.info(f"Kafka broker: %s", self.broker)
        
        self.catalog_url = f'http://{catalog_url}'
        LOGGER.info(f"AI Catalog URL: %s", self.catalog_url)
        
        self.models_index = models_index  # where to look for models in ElasticSearch
        LOGGER.info(f"Models index: %s", self.models_index)
        
        self.downloaded_models = {}  # we store the full path to the downloaded models so as not to download twice
        
        self.models_out_path = "./models/"
        LOGGER.info(f"Models output path: %s", self.models_out_path)

        self.producer_config = ProducerConfig(producer_client_id, producer_topic)
        self.consumer_config = ConsumerConfig(consumer_client_id, consumer_group_id, consumer_topic)
        
        LOGGER.info("Kafka Producer config: %s", self.producer_config)
        LOGGER.info("Kafka Consumer config: %s", self.consumer_config)

        # if output path does not exist, create it
        if not os.path.isdir(self.models_out_path):
            LOGGER.info("Creating models output directory: %s", self.models_out_path)
            os.mkdir(self.models_out_path)

        signal.signal(signal.SIGINT, self.__handler)

        self.producer, self.consumer = self.__connect_to_kafka()

        self.available_models_ids, self.available_models = self.list_models(self.catalog_url, self.models_index)
        
        if not self.available_models:
            LOGGER.error("%s: No models available", __name__)
            exit()
            
        LOGGER.info("Available models: %s", self.available_models)

        self.df_cols = None

        self.model, self.model_type, self.model_id, self.label_correspondence = self.select_model(load_model)

    def __handler(self, num, frame) -> None:
        LOGGER.info("Gracefully stopping...")
        self.consumer.close()
        self.producer.close()
        exit()

    def __connect_to_kafka(self) -> Tuple[KafkaProducer, KafkaConsumer]:
        """
        Creates and connects a KafkaProducer and a KafkaConsumer instance to the Kafka broker specified in self.broker
        Returns:
            A tuple consisting of the KafkaProducer and KafkaConsumer instances.
        """
        
        LOGGER.info("Attempting to establish connection to Kafka broker %s", self.broker)

        producer = KafkaProducer(bootstrap_servers=self.broker, client_id=self.producer_config.client_id,
                                 value_serializer=lambda x: pickle.dumps(x))
                                 
        consumer = KafkaConsumer(self.consumer_config.topic, bootstrap_servers=self.broker,
                                 client_id=self.consumer_config.client_id,
                                 group_id=self.consumer_config.group_id, value_deserializer=lambda x: pickle.loads(x))

        LOGGER.info("Trying to establish connection to brokers...")
        LOGGER.info("Producer connection status: %s", producer.bootstrap_connected())
        LOGGER.info("Consumer connection status: %s", consumer.bootstrap_connected())

        # Validate if connection to brokers is ready
        if not producer.bootstrap_connected():
            LOGGER.error("%s: Producer failed to connect to Kafka brokers.", __name__)
            exit()
            
        if not consumer.bootstrap_connected():
            LOGGER.error("%s: Consumer failed to connect to brokers.", __name__)
            exit()

        return producer, consumer

    def __send_data(self, probs: List[dict], metadatas: Any, version: str) -> None:
        """
        Publishes the output of the inference model to self.producer_topic.
        Args:
            probs: dictionary with the membership probability to each class
            metadatas: connection metadata
            version (str): list with the headers of the data.
        """
        
        data = {"data": probs, "metadata": metadatas, "label_correspondence": self.label_correspondence}
        
        try:
            self.producer.send(topic=self.producer_config.topic, value=data, headers=[("version", version.encode("utf-8"))],
                               timestamp_ms=time.time_ns() // 1000000)
            self.producer.flush()
            
        except Exception as e:
            LOGGER.error("%s: Error sending inference probabilities to Kafka cluster: %s", __name__, e)

    def list_models(self, model_repository_url: str, models_index: str) -> Tuple[List[str], List[Model]]:
        """
        Returns a list of the available models in self.models_index.
        Returns:
            List of models retrieved.
        """
        LOGGER.info("Getting available models from ElasticSearch...")
        headers = {'Content-Type': 'application/json'}

        api_url = (f'{model_repository_url}/{models_index}/_search?filter_path=hits.hits._id,'
                   f'hits.hits._source.file_data.filename,hits.hits._source.metadata')
        models = []
        ids = []
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            result = response.json()["hits"]["hits"]
            
            for model in result:
                model_id = model["_id"]
                filename = model["_source"]["file_data"]["filename"]
                metadata = model["_source"]["metadata"]
                models.append(Model(model_id, filename, metadata, None))
                ids.append(model_id)
                
        except requests.RequestException as e:
            LOGGER.error("%s: Error during request: %s", __name__, e)
            
        finally:
            return ids, models

    def get_model(self, model_repository_url: str, models_index: str, model_id: str) -> str:
        """
        Retrieves specified model from self.models_index and saves it as a local file.
        Args:
            models_index: ElasticSearch index where models are stored.
            model_repository_url: URL pointing to the model repository.
            model_id (str): The model's id in ElasticSearch index.
        Returns:
            Full path to the model.
        """
        
        LOGGER.info("Retrieving model (%s) from ElasticSearch...", model_id)
        headers = {'Content-Type': 'application/json'}

        api_url = f'{model_repository_url}/{models_index}/_doc/{model_id}'
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            result = response.json()

            filename = result["_source"]["file_data"]["filename"]
            file_content = result["_source"]["file_data"]["file_content"]
            metadata = result["_source"]["metadata"]
            file_content_bytes = b64decode(file_content.encode("utf-8"))

            full_path = os.path.join(self.models_out_path, filename)

            with open(full_path, "wb") as f:
                f.write(file_content_bytes)

            if full_path not in self.downloaded_models:
                self.downloaded_models[model_id] = full_path

            LOGGER.info("Model %s (%s) was successfully downloaded", filename, model_id)
            
            return full_path
            
        except requests.RequestException as e:
            LOGGER.error("%s: Error during request: %s", __name__, e)

    def __load_model_from_file(self, path_to_model: str) -> Tuple[Any, str]:        
        LOGGER.info("Loading model...")
        
        if path_to_model.endswith("pkl"):
            model = pickle.load(open(path_to_model, "rb"))
            model_type = "RF"
            
        elif path_to_model.endswith("joblib"):
            model = load(path_to_model)
            model_type = "RF"
            
        else:
            LOGGER.error("%s: Model format not supported.", __name__)
            
            return None, "error"
            
        LOGGER.info("Model %s was successfully loaded.", path_to_model)
        
        return model, model_type

    def select_model(self, model_id: str) -> Tuple[Any, str]:
        model_is_valid = True
        
        if model_id is None or model_id == "None":
            LOGGER.warning("No model was selected.")
            
            model_is_valid = False
            
        elif model_id not in self.available_models_ids:
            LOGGER.warning("Model (%s) not available", model_id)
            
            model_is_valid = False
            
        if not model_is_valid:
            model_id = self.available_models_ids[0]
            
            LOGGER.info("Selecting first available model (%s)...", model_id)

        if model_id in self.downloaded_models:
            model_path = self.downloaded_models[model_id]
            
        else:
            model_path = self.get_model(self.catalog_url, self.models_index, model_id)
            
        model, model_type = self.__load_model_from_file(model_path)
        
        label_correspondence = self.available_models[self.available_models_ids.index(model_id)].metadata["label_correspondence"]
        
        label_correspondence = {int(i): label_correspondence[i] for i in label_correspondence}
        
        return model, model_type, model_id, label_correspondence

    def __predict(self, model: Any, df: pd.DataFrame, version: str) -> np.ndarray:
        """
        TO-DO
        """
        if len(df) == 0:
            return None
            
        probs = []
        
        if version == "v1":
            probs = model.predict_proba(df)
            
        elif version == "v1_point_5":
            pass
            
        elif version == "v2":
            pass
            
        elif version == "v3_1_A":
            pass
            
        elif version == "v3_1_B":
            pass
            
        elif version == "v3_2_A":
            pass
            
        elif version == "v3_2_B":
            pass
            
        elif version == "v3_3_A":
            pass
            
        elif version == "v3_3_B":
            pass
            
        return probs

    def __parse_message(self, message: Any, features_names: List[str], version: str) -> Tuple[pd.DataFrame, list]:
        # OPTIMIZAR / REESTRUCTURAR ?
        features = []
        #metadata = []
        
        if version == "v1":
            data = message["data"]
            message_metadata = message["metadata"]
            
            # en connection_id están ip_src, ip_dst, port_src, port_dst y first
            metadata = {**message_metadata['connection_id'], 'timestamp': message_metadata['timestamp'], 'timestamp_data_aggregator': message_metadata['timestamp_data_aggregator'],
                        'timestamp_inference': datetime.timestamp(datetime.now()), 'flow_bytes': message_metadata['flow_bytes'], 'flow_pkts': message_metadata['flow_pkts'],
                        'monitored_device': message_metadata['monitored_device'], 'interface': message_metadata['interface']}
                             
            #LOGGER.info("Diferencia de tiempo entre data_aggregator y ai-inference: %s", datetime.timestamp(datetime.now())-message_metadata['timestamp_after_process'])
            
            features.append({**data['features']})
            
        """elif version == "v1_point_5" or version == "v2":
            for snapshot in message:
                data = snapshot["data"]
                message_metadata = snapshot["metadata"]
                
                features.append({**data['features']})
                
            metadata.append({**message_metadata['connection_id'], 'timestamp': message_metadata['timestamp'], 'flow_bytes': message_metadata['flow_bytes'], 'flow_pkts': message_metadata['flow_pkts'],
                             'monitored_device': message_metadata['monitored_device'], 'interface': message_metadata['interface']})
                             
        elif version in ("v3_1_A", "v3_1_B", "v3_2_A", "v3_2_B"):
            for snapshot in message:
                data = snapshot["data"]
                message_metadata = snapshot["metadata"]
                
                metadata.append({**message_metadata['connection_id'], 'timestamp': message_metadata['timestamp'], 'flow_bytes': message_metadata['flow_bytes'],
                                 'flow_pkts': message_metadata['flow_pkts'], 'monitored_device': message_metadata['monitored_device'], 'interface': message_metadata['interface']})
                                 
                features.append({**data['features']})
                
        elif version == "v3_3_A" or version == "v3_3_B":
            for window in message:
                last = None
                window_features = []
                
                for snapshot in window:
                    if snapshot != "0":
                        data = snapshot["data"]
                        message_metadata = snapshot["metadata"]
                        window_features.append({**data['features']})
                        last = message_metadata
                        
                    else:
                        window_features.append({i: "-1" for i in features_names})
                        
                if last is not None:
                    metadata.append({**last['connection_id'], 'timestamp': last['timestamp'], 'flow_bytes': last['flow_bytes'], 'flow_pkts': last['flow_pkts'],
                                     'monitored_device': last['monitored_device'], 'interface': last['interface']})
                                     
                    features.append(window_features)
                    
        else:
            LOGGER.error("%s: Version %s not supported", __name__, version)
            exit()"""
            
        return pd.DataFrame(features), metadata

    def start_inference(self) -> None:
        """
        Starts data inference.
        """
        
        LOGGER.info("Starting data inference...")
        
        if self.model is None:
            LOGGER.error("%s: No model was loaded", __name__)
            
            return
            
        features_buffer = []
        metadata_buffer = []
        
        """last_time_received = time.time()
        
        t_predicts = []
        n_predicts = 0"""
        
        while True:
            messages = self.consumer.poll()
            
            if messages:
                messages = list(messages.values())[0]
                
                for message in messages:
                    snapshots = message.value
                    LOGGER.info("%s received snapshots", len(snapshots))
                    version = message.headers[0][1].decode("utf-8")
                    features_names = message.headers[1][1].decode("utf-8").split(", ")
                    for snapshot in snapshots:
                        features, metadata = self.__parse_message(snapshot, features_names, version)
                        features_buffer.append(features)
                        metadata_buffer.append(metadata)
                        
                    probs = self.__predict(self.model, pd.concat(features_buffer, ignore_index=True), version)
                    
                    LOGGER.info("Predicted %s snapshots.", len(features_buffer))
                    
                    if probs is not None:
                        self.__send_data(probs, metadata_buffer, version)
                        
                    features_buffer = []
                    metadata_buffer = []
                            
                """        if len(features_buffer)>=BUFFER_SIZE:
                            start = time.time()
                            
                            probs = self.__predict(self.model, pd.concat(features_buffer, ignore_index=True), version)
                            
                            LOGGER.info("Predicted %s snapshots.", len(features_buffer))
                            
                            n_predicts += 1
                            t_predicts.append(time.time()-start)
                            #LOGGER.info("Tiempo medio de prediccion: %s", sum(t_predicts)/len(t_predicts))
                            #LOGGER.info("Numero de predicciones: %s", n_predicts)
                            
                            if probs is not None:
                                self.__send_data(probs, metadata_buffer, version)
                                
                            features_buffer = []
                            metadata_buffer = []
                        
                LOGGER.info("Cantidad de mensajes: %s", len(messages))
                LOGGER.info("Tiempo entre mensajes: %s", time.time()-last_time_received)
                last_time_received = time.time()"""
                
            else:
                time.sleep(0.0001)
                """if time.time()>=last_time_received+TIMEOUT and features_buffer:
                    LOGGER.info("Timeout exceeded. Predicting %s snapshots...", len(features_buffer))
                    start = time.time()
                        
                    probs = self.__predict(self.model, pd.concat(features_buffer, ignore_index=True), version)
                    
                    n_predicts += 1
                    t_predicts.append(time.time()-start)
                    #LOGGER.info("Tiempo medio de prediccion: %s", sum(t_predicts)/len(t_predicts))
                    #LOGGER.info("Numero de predicciones: %s", n_predicts)
                    
                    if probs is not None:
                        self.__send_data(probs, metadata_buffer, version)
                            
                    features_buffer = []
                    metadata_buffer = []"""


def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """

    ai_inference = AIInference(kafka_url=args.kafka_url, catalog_url=args.catalog_url,
                               models_index=args.catalog_models_index, consumer_topic=args.consumer_topic,
                               producer_topic=args.producer_topic, consumer_client_id=args.consumer_client_id,
                               producer_client_id=args.producer_client_id, consumer_group_id=args.consumer_group_id)
    ai_inference.start_inference()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start AI Inference component.")
    parser.add_argument("--kafka_url", type=str, default='localhost:9094',
                        help="IP address and port of the Kafka cluster ('localhost:9094' by default).")
    parser.add_argument("--catalog_url", type=str, default='localhost:9200',
                        help="IP address and port of the Catalog component ('localhost:9200' by default).")
    parser.add_argument("--consumer_topic", type=str, default='inference_data',
                        help="Kafka topic which the consumer will be consuming from.")
    parser.add_argument("--producer_topic", type=str, default='inference_probs',
                        help="Kafka topic which the producer will be publishing to.")
    parser.add_argument("--consumer_client_id", type=str, default='ai-inference-consumer',
                        help="ID of the consumer client by which it will be recognized within Kafka.")
    parser.add_argument("--producer_client_id", type=str, default='ai-inference-producer',
                        help="ID of the producer client by which it will be recognized within Kafka.")
    parser.add_argument("--consumer_group_id", type=str, default='ai-inference',
                        help="ID of the consumer group which the consumer belongs to.")
    parser.add_argument("--catalog_models_index", type=str, default='models',
                        help="ElasticSearch index in which prediction models will be stored.")
    parser.add_argument("--load_model", type=str, default=None,
                        help="ElasticSearch id of the model to use for inference.")

    args = parser.parse_args()
    main(args)
