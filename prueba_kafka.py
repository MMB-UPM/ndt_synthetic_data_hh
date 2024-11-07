from typing import Tuple, Any
import logging
from kafka import KafkaConsumer, KafkaProducer
from sys import stdout, exit
import time
from collections import namedtuple, defaultdict
import signal
import argparse
import pickle
import numpy as np
from datetime import datetime

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic"])

class AIDetector:
    def __init__(self, kafka_url: str, consumer_client_id: str, consumer_group_id: str,
                 consumer_topic: str):
        LOGGER.info("Initializing AI Detector...")

        self.broker = kafka_url
        LOGGER.info(f"Kafka broker: %s", self.broker)

        self.consumer_config = ConsumerConfig(consumer_client_id, consumer_group_id, consumer_topic)
        
        LOGGER.info("Kafka Consumer config: %s", self.consumer_config)

        signal.signal(signal.SIGINT, self.handler)

        self.consumer = self.connect_to_kafka()

    def handler(self, num, frame) -> None:
        LOGGER.info("Gracefully stopping...")
        
        self.consumer.close()
        exit()

    def connect_to_kafka(self):
        """
        Creates and connects a KafkaProducer and a KafkaConsumer instance to the Kafka broker specified in self.broker
        Returns:
            A tuple consisting of the KafkaProducer and KafkaConsumer instances.
        """
        
        LOGGER.info("Attempting to establish connection to Kafka broker %s", self.broker)
                                 
        consumer = KafkaConsumer(self.consumer_config.topic, bootstrap_servers=self.broker,
                                 client_id=self.consumer_config.client_id,
                                 group_id=self.consumer_config.group_id, value_deserializer=lambda x: pickle.loads(x))

        LOGGER.info("Trying to establish connection to brokers...")
        LOGGER.info("Consumer connection status: %s", consumer.bootstrap_connected())

        if not consumer.bootstrap_connected():
            LOGGER.error("%s: Consumer failed to connect to brokers.", __name__)
            exit()

        return consumer

    def start_detection(self) -> None:
        """
        Starts detection.
        """
        
        LOGGER.info("Starting detection...")
        
        while True:
            messages = self.consumer.poll()
            
            if messages:
                messages = list(messages.values())[0]
                
                for message in messages:
                    print(message)      
            else:
                time.sleep(0.0001)
            
            
def main(args):
    """
    Main function for setting up and initialize DDOS detection.
    Args:
        args (Any): Command-line arguments and options.
    """

    ai_detector = AIDetector(kafka_url=args.kafka_url, consumer_topic=args.consumer_topic, consumer_client_id=args.consumer_client_id, consumer_group_id=args.consumer_group_id)
    ai_detector.start_detection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start AI Detector component.")
    parser.add_argument("--kafka_url", type=str, default='localhost:9094',
                        help="IP address and port of the Kafka cluster ('localhost:9094' by default).")
    parser.add_argument("--consumer_topic", type=str, default='inference_probs',
                        help="Kafka topic which the consumer will be consuming from.")
    parser.add_argument("--consumer_client_id", type=str, default='ai-detector-consumer',
                        help="ID of the consumer client by which it will be recognized within Kafka.")
    parser.add_argument("--consumer_group_id", type=str, default='ai-detector',
                        help="ID of the consumer group which the consumer belongs to.")

    

    args = parser.parse_args()
    main(args)