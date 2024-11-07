import logging
from sys import stdout
import os
import time
import signal
import numpy as np
import json
import sys
import pickle
import random
from collections import defaultdict
from kafka import KafkaProducer
from typing import Generator, IO, List, Tuple, Dict, Any
from datetime import datetime
from pytz import timezone

# Setup LOGGER
LOGGER = logging.getLogger("dad_LOGGER")
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt="%(levelname)-8s %(message)s")
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

MAX_SNAPSHOTS = 300
K = 0.3
TIME_INTERVAL = 1.0  # en segundos

class DataAggregator():
    def __init__(self, feature_ids, topic:str, window_size:int, protocol:str, version:str, interval_seconds:int, num_connections:int,
                 timeout:float, mode:str, broker_ip_address:str, broker_port:str, tstat_dir_name:str, ignore_first_line_tstat:bool,
                 monitored_device:str, interface:str):
        LOGGER.info("Creating Data Aggregator")
        self.feature_ids = feature_ids
        self.topic = topic
        self.window_size = window_size
        self.protocol = protocol
        self.version = version
        self.interval_seconds = interval_seconds
        self.num_connections = num_connections
        self.timeout = timeout
        self.mode = mode
        self.tstat_dir_name = os.path.join(tstat_dir_name, "")
        self.ignore_first_line_tstat = ignore_first_line_tstat
        self.producer = self.__connect_publisher_client(broker_ip_address, broker_port)
        self.monitored_device = monitored_device
        self.interface = interface
        self.window = defaultdict(list)
        self.first_snapshot_time = {}
        self.interval_dict = {}
        self.timeout_dict = {}
        self.connection_order = []
        self.matrix = ['0'] * self.num_connections
        self.matrix_window = []
        self.prev_time = {}
        self.prev_timestamp_pcap = 0.0
        self.prev_timestamp_real = 0.0

        
        signal.signal(signal.SIGINT, self.__handler)
            
        
    def __handler(self, num, frame):
        """
            Handles user stopping.
        """
        LOGGER.info("Gracefully stopping...")
        #self.producer.close()
        exit()
    
    def __follow(self, thefile: IO[str], time_sleep:float) -> Generator[str, None, None]:
        """
            Generator function that yields new lines from a file.
            Args:
                thefile (IO[str]): The file to be followed by this method.
                time_sleep (float): Time to be asleep if the file hasn't been updated or is still empty.
            Returns:
                A Generator for the file.
        """

        trozo = ""

        # start infinite loop
        while True:
            # read last line of file
            line = thefile.readline()

            # sleep if file hasn't been updated
            if not line:

                time.sleep(time_sleep)
                continue
            if line[-1] != "\n":
                trozo += line
            else:
                if trozo != "":
                    line = trozo + line
                    trozo = ""
                yield line


    def __load_file(self) -> str:
        """
            Retrieves the tstat generated CSV file.
            Returns:
                Path to the file.
        """
        while True:
            tstat_dirs = os.listdir(self.tstat_dir_name)
            if len(tstat_dirs) > 0:
                tstat_dirs.sort()
                tstat_file = self.tstat_dir_name + 'log_tcp_temp_complete' 
                LOGGER.info("Following: {0}".format(tstat_file))
                return tstat_file
            else:
                LOGGER.info("No Tstat directory!")
                time.sleep(5)
    
    def __split_line(self, line:str) -> List[str]:
        """
            Splits a CSV line in parts separated by " ".
            Args:
                line (str): line to be splitted.
            Returns:
                List of parts of the line.
        """
        line_splitted = line.split(" ")
        return line_splitted
    
    def __get_first(self, line:str) -> float:
        """
            Retrieves connection identifiers.
            Args:
                line (str): snapshot.
            Returns:
                Flow first packet absolute time.
        """
        line_splitted = self.__split_line(line)
        return float(line_splitted[28])
    
    def __get_conn_id(self, line:str) -> Tuple[str, str, str, str]:
        """
            Retrieves connection identifiers.
            Args:
                line (str): snapshot.
            Returns:
                A tuple of: source ip, source port, destination ip, destination port.
        """
        line_splitted = self.__split_line(line)
        conn_id = (line_splitted[0], line_splitted[1], line_splitted[14], line_splitted[15])
        return conn_id
    
    def __get_flow_packets(self, line:str) -> int:
        """
            Retrieves total number of flow packets (in and out).
            Args:
                line (str): snapshot.
            Returns:
                The sum of the fields "c_pkts_all" and "s_pkts_all".
        """
        line_splitted = self.__split_line(line)
        flow_packets = int(line_splitted[2]) + int(line_splitted[16])
        return flow_packets
    
    def __get_flow_bytes(self, line:str) -> int:
        """
            Retrieves total number of flow bytes (in and out).
            Args:
                line (str): snapshot.
            Returns:
                The sum of the fields "c_bytes_uniq" and "s_bytes_uniq".
        """
        line_splitted = self.__split_line(line)
        flow_bytes = int(line_splitted[6]) + int(line_splitted[20])
        return flow_bytes
    
    def __get_last_time_field(self, line:str) -> float:
        """
            Retrieves the timestamp of the last packet sent in the snapshot.
            Args:
                line (str): snapshot.
            Returns:
                The field "Last time abs ms Flow last segment absolute time (epoch)" of the CSV line.
        """
        line_splitted = self.__split_line(line)
        current_time = float(line_splitted[29])
        return current_time
    
    def __get_json(self, conn_id:Tuple[str, str, str, str], flow_packets:int, flow_bytes:int, feature_dict:dict, current_time:float, first:float) -> Dict[str, Any]:
        """
            Returns a Json like dict which stores the specified features and information to be sent.
            Args:
                conn_id (Tuple[str, str, str, str]): snapshot's connection iAnyd.
                flow_packets (int): total number of flow packets.
                flow_bytes (int): total number of flow bytes.
                feature_dict (dict): selected features.
                current_time (float): timestamp for the snapshot.
            Returns:
                Dict of str type keys (Json type dict).
        """

        try:
            data_dict = {
                "data":{
                    "features":feature_dict,
                },
                "metadata":{
                    "timestamp": current_time,
                    "timestamp_data_aggregator": datetime.timestamp(datetime.now()),
                    "monitored_device": self.monitored_device,
                    "interface": self.interface,
                    "connection_id":{
                        "src_ip": conn_id[0],
                        "dst_ip": conn_id[2],
                        "src_port": conn_id[1],
                        "dst_port": conn_id[3],
                        "protocol": self.protocol,
                        "first": first,
                    },
                    "flow_pkts": flow_packets,
                    "flow_bytes": flow_bytes,
                }
            }

        except IndexError:
            LOGGER.error("IndexError: {0}".format(conn_id))
        
        return data_dict
    
    def __get_time(self, line:str) -> float:
        """
            Retrieves either the current time or the timestamp of the last packet sent in the snapshot.
            Args:
                line (str): snapshot.
            Returns:
                Current time or the field "Last time abs ms Flow last segment absolute time (epoch)" of the CSV line.
        """
        if self.mode == 'streaming': 
            current_time = self.__get_last_time_field(line)
        elif self.mode == 'static':
            current_timestamp_pcap = self.__get_last_time_field(line) / 1000
            if self.prev_timestamp_real == 0:
                current_time = datetime.timestamp(datetime.now(timezone("Europe/Madrid")))
            else:
                current_time = self.prev_timestamp_real + (current_timestamp_pcap - self.prev_timestamp_pcap)
            #LOGGER.info("current_timestamp_pcap=%s, current_timestamp=%s", current_timestamp_pcap, current_time)
            #LOGGER.info("current_datetime=%s",datetime.fromtimestamp(current_time))
            self.prev_timestamp_pcap = current_timestamp_pcap
            self.prev_timestamp_real = current_time
        else:
            sys.exit("Selected Mode does not exist.")
        
        return current_time


    def __process_v1(self, line:str, feature_names:list) -> None:
        """
            Precesses a snapshot for Version 1.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                prev_timestamp_pcap (float): timestamp from field "last" of previous snapshot.
                prev_timestamp_real (float): timestamp assigned to previous snapshot.
        """
        line_splitted = self.__split_line(line)
        conn_id = self.__get_conn_id(line)
        flow_packets = self.__get_flow_packets(line)
        flow_bytes = self.__get_flow_bytes(line)
        features_dict = {word: line_splitted[idx] for idx, word in feature_names}
        current_time = self.__get_time(line)
        first = self.__get_first(line)

        return self.__get_json(conn_id, flow_packets, flow_bytes, features_dict, current_time, first)
    
    def __process_v1_point_5(self, line:str, feature_names:list, delta:float) -> Dict[str, Any]:
        """
            Precesses a snapshot for Version 1.5 .
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                delta (float): time delta between current and last snapshot from same connection.
            Returns:
                Dict of str type keys (Json type dict).
        """
        line_splitted = self.__split_line(line)
        conn_id = self.__get_conn_id(line)
        features_dict = {word: line_splitted[idx] for idx, word in feature_names}
        features_dict["delta"] = str(delta)
        
        
        flow_packets = self.__get_flow_packets(line)
        flow_bytes = self.__get_flow_bytes(line)
        current_time = self.__get_time(line)
        first = self.__get_first(line)
        
        json_data = self.__get_json(conn_id, flow_packets, flow_bytes, features_dict, current_time, first)
        
        return json_data
    
    def __get_delta(self, line:str) -> Tuple[float, Tuple[str, str, str, str]]:
        """
            Calculates delta between current and last snapshot from same connection.
            Args:
                line (str): snapshot.
            Returns:
                A tuple of: delta and connection id
        """
        conn_id = self.__get_conn_id(line)
        current_time = self.__get_last_time_field(line)

        if conn_id in self.prev_time:
            delta = current_time - float(self.prev_time[conn_id])
        else:
            delta = 0.0
        
        self.prev_time[conn_id] = current_time

        return delta, conn_id

    def __process_v2(self, line:str, feature_names:list) -> None:
        """
            Precesses a snapshot for Version 2.
            Sends windows of snapshots from same connection to the Kafka topic in uniform time intervals.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
        """
        conn_id = self.__get_conn_id(line)
        current_time = self.__get_last_time_field(line)
        processed_line = self.__process_v1(line, feature_names)

        if conn_id in self.first_snapshot_time:
            current_interval = int((current_time - float(self.first_snapshot_time[conn_id]))/self.interval_seconds)      

            if current_interval > self.interval_dict[conn_id]+1:
                repeat = (current_interval - self.interval_dict[conn_id]+1)%self.window_size

                if len(window[conn_id]) == self.window_size:
                    window_serialized = pickle.dumps(self.window[conn_id])
                    self.__publish_data(window_serialized, feature_names)
                for _ in range(repeat):
                    self.interval_dict[conn_id] += 1

                    if len(self.window[conn_id]) == self.window_size:
                        self.window[conn_id].pop(0)
                        
                    self.window[conn_id].append(self.window[conn_id][-1])

                    if len(self.window[conn_id]) == self.window_size:
                        window_serialized = pickle.dumps(self.window[conn_id])
                        self.__publish_data(window_serialized, feature_names)

                if len(self.window[conn_id]) == self.window_size:
                    self.window[conn_id].pop(0)
                self.window[conn_id].append(processed_line)
                self.timeout_dict[conn_id] = self.__get_time(line)
                self.interval_dict[conn_id] += 1

            elif current_interval == self.interval_dict[conn_id]+1:
                self.interval_dict[conn_id] += 1

                if len(self.window[conn_id]) == self.window_size:
                    window_serialized = pickle.dumps(self.window[conn_id])
                    self.__publish_data(window_serialized, feature_names)
                    self.window[conn_id].pop(0)
                self.window[conn_id].append(processed_line)  
                self.timeout_dict[conn_id] = self.__get_time(line)
            else:
                self.window[conn_id].pop(-1)
                self.window[conn_id].append(processed_line)
                self.timeout_dict[conn_id] = self.__get_time(line)
        else:
            self.first_snapshot_time[conn_id] = current_time
            self.interval_dict[conn_id] = 0
            self.window[conn_id].append(processed_line)
            self.timeout_dict[conn_id] = self.__get_time(line)
    
    def __process_v3_1_and_2_A(self, line:str ,feature_names:list, 
                               last_time:float) -> float:
        """
            Precesses a snapshot for Versions 3.1 A and 3.2 A.
            Sends a matrix including every connection's last sapshot in current interval to the Kafka topic.
            Maintains consistency regarding connections order within the matrix.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                last_time (float): time of the last matrix sent.
            Returns:
                next_last_time.
        """
        conn_id = self.__get_conn_id(line)
        processed_line = self.__process_v1(line, feature_names)
        current_time = self.__get_time(line)

        next_last_time = last_time

        if current_time - last_time >= self.interval_seconds:
            matrix_serialized = pickle.dumps(self.matrix)
            self.__publish_data(matrix_serialized, feature_names)
            next_last_time = current_time
            self.matrix = ['0'] * self.num_connections
        
        if conn_id not in self.connection_order:
            self.connection_order.append(conn_id)
            
        self.matrix[self.connection_order.index(conn_id)] = processed_line
        
        return next_last_time
    
    def __process_v3_1_and_2_B(self, line:str ,feature_names:list,
                               last_time:float) -> float:
        """
            Precesses a snapshot for Versions 3.1 B and 3.2 B.
            Sends a matrix including every connection's last sapshot in current interval to the Kafka topic.
            Does NOT maintain consistency regarding connections order within the matrix.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                last_time (float): time of the last matrix sent.
            Returns:
                next_last_time.
        """
        conn_id = self.__get_conn_id(line)
        processed_line = self.__process_v1(line, feature_names)
        current_time = self.__get_time(line)
        
        next_last_time = last_time

        if current_time - last_time >= self.interval_seconds:

            matrix_serialized = pickle.dumps(self.matrix)
            self.__publish_data(matrix_serialized, feature_names)
            next_last_time = current_time
            self.matrix = ['0'] * self.num_connections
            self.connection_order.clear()
        
        if conn_id not in self.connection_order:
            self.connection_order.append(conn_id)
        
        self.matrix[self.connection_order.index(conn_id)] = processed_line

        return next_last_time
    
    def __process_v3_3_A(self, line:str ,feature_names:list,
                         last_time:float) -> float:
        """
            Precesses a snapshot for Version 3.3 A.
            Sends a window of matrixes including every connection's last sapshot in last self.window_size intervals to the Kafka topic.
            Maintains consistency regarding connections order within the matrix.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                last_time (float): time of the last matrix sent.
            Returns:
                next_last_time.
        """
        conn_id = self.__get_conn_id(line)
        processed_line = self.__process_v1(line, feature_names)
        current_time = self.__get_time(line)

        next_last_time = last_time

        if current_time - last_time >= self.interval_seconds:
            if len(self.matrix_window) == self.window_size:
                self.matrix_window.pop(0)
            self.matrix_window.append(self.matrix)
            if len(self.matrix_window) == self.window_size:
                window_serialized = pickle.dumps(self.matrix_window)
                self.__publish_data(window_serialized, feature_names)
            next_last_time = current_time
            self.matrix = ['0'] * self.num_connections
            
        if conn_id not in self.connection_order:
            self.connection_order.append(conn_id)
        
        self.matrix[self.connection_order.index(conn_id)] = processed_line
        
        return next_last_time
    
    def __process_v3_3_B(self, line:str , feature_names:list,
                         last_time:float) -> float:
        """
        Precesses a snapshot for Version 3.3 B.
            Sends a window of matrixes including every connection's last sapshot in last self.window_size intervals to the Kafka topic.
            Does NOT maintain consistency regarding connections order within the matrix.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                last_time (float): time of the last matrix sent.
            Returns:
                next_last_time.
        """
        conn_id = self.__get_conn_id(line)
        processed_line = self.__process_v1(line, feature_names)
        current_time = self.__get_time(line)

        next_last_time = last_time

        if current_time - last_time >= self.interval_seconds:
            if len(self.matrix_window) == self.window_size:
                self.matrix_window.pop(0)
            self.matrix_window.append(self.matrix)
            if len(self.matrix_window) == self.window_size:
                window_serialized = pickle.dumps(self.matrix_window)
                self.__publish_data(window_serialized, feature_names)
            next_last_time = current_time
            self.matrix = ['0'] * self.num_connections
            self.connection_order.clear()
    
        if conn_id not in self.connection_order:
            self.connection_order.append(conn_id)

        self.matrix[self.connection_order.index(conn_id)] = processed_line

        return next_last_time
    
    def __connect_publisher_client(self, broker_ip_address:str, broker_port:str) -> KafkaProducer:
        """
            Creates a KafkaProducer instance and returns it.
            Args:
                broker_ip_address (str): kafka broker's ip address.
                broker_port (str): kafka broker's port.
            Returns:
                KafkaProducer instance.

        """         
        broker = f'{broker_ip_address}:{broker_port}'
        producer = KafkaProducer(
            bootstrap_servers=broker
        )

        LOGGER.info("Trying to establish connection to brokers...")

        if not producer.bootstrap_connected():
            sys.exit("Failed to connect to brokers.")

        return producer


    def __publish_data(self, window:bytes, feature_names:list) -> None:
        '''
            Sends a pickled representetation to the kafka topic.
            Args:
                window (bytes): pickled representation of an object.
                feature_names (list): features to send.
        '''
        version_bytes = self.version.encode('utf-8')

        indices = [name for _, name in feature_names]
        features = ", ".join(indices)
        features_bytes = features.encode('utf-8')
        
        self.producer.send(
            headers= [("version", version_bytes), ("features", features_bytes)],
            topic=self.topic,
            value=window,
            timestamp_ms=time.time_ns() // 1000000
        )

        self.producer.flush()


    def send_data(self) -> None:
        """
            Manages the traffic and sends the appropiate data in the right way, depending on the different specified parameters of the class.
        """
        LOGGER.info("Loading Tstat log file...")
        logfile = open(self.__load_file(), "r")

        LOGGER.info("Following Tstat log file...")
        loglines = self.__follow(logfile, 5)

        process_time = []
        num_lines = 0
        last_time = 0.0
        interval_start = 0.0
        interval_end = 0.0
        
        processed = 0
        last_processed = 0
        not_processed = 0
        last_not_processed = 0
        n_intervals = 0
        
        last_snapshot_per_connection = dict()
        
        counter = 0

        LOGGER.info("Starting to process data...")
        
        while True:
            line = next(loglines, None)
            while line == None:
                LOGGER.info("Waiting for new data...")
                time.sleep(1 / 100)
                line = next(loglines, None)
            if num_lines == 0 and self.ignore_first_line_tstat:
                num_lines += 1
                feature_names = [(idx, word) for idx, word in enumerate(line.split()) if idx in self.feature_ids]
                feature_names = [(num, name.split(":")[0]) for num, name in feature_names]
                continue
            
            #if (self.__get_flow_packets(line))>13:
            if last_time == 0.0:
                last_time = datetime.timestamp(datetime.now())
                interval_start = last_time
                interval_end = interval_start + TIME_INTERVAL
                
            start = time.time()

            if self.version == 'v1':
                snapshot = self.__process_v1(line, feature_names)
                #LOGGER.info(snapshot['metadata']['connection_id'])
                
                if snapshot['metadata']['timestamp'] >= interval_end:
                    if last_snapshot_per_connection:
                        n_intervals += 1
                        processed += len(last_snapshot_per_connection)
                        #LOGGER.info("Interval %s ended. %s different connections", (datetime.fromtimestamp(interval_start), datetime.fromtimestamp(interval_end)), len(last_snapshot_per_connection))
                        #LOGGER.info("Number of UNPROCESSED snapshots in this interval: %s", not_processed-last_not_processed)
                        #LOGGER.info("Total number of UNPROCESSED snapshots: %s", not_processed)
                        #LOGGER.info("Mean number of UNPROCESSED snapshots per interval: %s", not_processed/n_intervals)
                        #LOGGER.info("Number of PROCESSED snapshots in this interval: %s", processed-last_processed)
                        #LOGGER.info("Total number of PROCESSED snapshots: %s", processed)
                        #LOGGER.info("Mean number of PROCESSED snapshots per interval: %s", processed/n_intervals)
                        
                        last_not_processed = not_processed
                        last_processed = processed
                        
                        counter += len(last_snapshot_per_connection)
                        print(counter)
                        
                        if len(last_snapshot_per_connection)>MAX_SNAPSHOTS:
                            #LOGGER.info("More than %s unique connections. Splitting...", MAX_SNAPSHOTS)
                            
                            for i in range(0, len(last_snapshot_per_connection), MAX_SNAPSHOTS):
                                self.window = list(last_snapshot_per_connection.values())[i:i+MAX_SNAPSHOTS]
                                window_serialized = pickle.dumps(self.window)
                                self.__publish_data(window_serialized, feature_names)
                                
                        else:
                            self.window = list(last_snapshot_per_connection.values())
                            window_serialized = pickle.dumps(self.window)
                            self.__publish_data(window_serialized, feature_names)
                    
                    last_snapshot_per_connection = dict()
                    interval_start = interval_end
                    interval_end += TIME_INTERVAL
                    
                #LOGGER.info(last_snapshot_per_connection)
                if random.random() < K:
                    if tuple(snapshot['metadata']['connection_id'].values()) in last_snapshot_per_connection:
                        not_processed += 1
                    last_snapshot_per_connection[tuple(snapshot['metadata']['connection_id'].values())] = snapshot
                else:
                    not_processed += 1
                #last_snapshot_per_connection[tuple(snapshot['metadata']['connection_id'].values())] = snapshot
                #print(counter)
                

            elif self.version == 'v1_point_5':
                delta, conn_id = self.__get_delta(line)
                line_processed = self.__process_v1_point_5(line, feature_names, delta)
                if len(self.window[conn_id]) == self.window_size:
                    self.window[conn_id].pop(0)

                self.window[conn_id].append(line_processed)
                if len(self.window[conn_id]) == self.window_size:
                    window_serialized = pickle.dumps(self.window[conn_id])
                    self.__publish_data(window_serialized, feature_names)


            elif self.version == 'v2':
                self.__process_v2(line, feature_names)
                current_time = self.__get_time(line)
                for connection, snapshots in self.window.items():
                    if len(snapshots) < self.window_size and current_time - self.timeout_dict[connection] >= self.timeout:
                        repeat = self.window_size-len(snapshots)
                        for _ in range(repeat):
                            self.window[connection].append(self.window[connection][-1])
                        window_serialized = pickle.dumps(self.window[connection])
                        self.__publish_data(self.window_serialized, feature_names)
                

            elif self.version == 'v3_1_A' or self.version == 'v3_2_A':
                last_time = self.__process_v3_1_and_2_A(line, feature_names, last_time)
            elif self.version == 'v3_1_B' or self.version == 'v3_2_B':
                last_time = self.__process_v3_1_and_2_B(line, feature_names, last_time)
                    
            elif self.version == 'v3_3_A':
                last_time = self.__process_v3_3_A(line, feature_names, last_time)
            
            elif self.version == 'v3_3_B':
                last_time = self.__process_v3_3_B(line, feature_names, last_time)

            else:
                sys.exit("Selected Version does not exist.")
                
            process_time.append(time.time() - start)
	        
            num_lines += 1
            if num_lines % 1000 == 0:
                LOGGER.info(f"Number of snapshots: {num_lines} - Average processing time of last 1000 snapshots: {sum(process_time) / 1000}")
                process_time = []
	            #metrics = self.producer.metrics()
	            #LOGGER.info(f"Metrics: {metrics}")

                #randint_difference = randint_start - randint_end
                #LOGGER.info(f"Time to do random.randint(1, 100): {randint_difference}")
            if num_lines > 38989000:
                print(counter+len(last_snapshot_per_connection))
                print(line)
            """else:
                LOGGER.info("Less than 13 packets")
                print(counter)
                continue"""
