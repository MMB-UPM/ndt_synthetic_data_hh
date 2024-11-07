import logging
from sys import stdout
import sys
from data_aggregator import DataAggregator
import argparse

# Setup logging
LOGGER = logging.getLogger("data_aggregator_logger")
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt="%(levelname)-8s %(message)s")
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """

    data_aggretator = DataAggregator(feature_ids= args.features, topic=args.producer_topic, window_size=args.window_size,
                                     protocol= args.protocol, version= args.version, interval_seconds=args.interval,
                                     num_connections= args.num_connections, timeout=args.timeout, mode= args.mode,
                                     broker_ip_address= args.kafka_ip, broker_port=args.kafka_port, tstat_dir_name= args.tstat_dir,
                                     ignore_first_line_tstat= args.ignore_first, monitored_device= args.monitored_device,
                                     interface= args.interface)
    data_aggretator.send_data()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start Data Aggregator component.")
    parser.add_argument("--kafka-ip", type=str, default='localhost', help="IP address of the Kafka cluster ('kafka' by default).")
    parser.add_argument("--kafka-port", type=str, default='9094', help="Port of the Kafka cluster ('9092' by default).")
    parser.add_argument("--producer-topic", type=str, default='inference_data', help="Kafka topic which the producer will be publishing to.")
    parser.add_argument("--tstat-dir", type=str, default= "/piped/2024_04_16_00_15.out/", help="Path to Tstat output directory.")
    parser.add_argument("--ignore-first", action='store_true', help="Ignore first line of Tstat's output CSV file.")
    parser.add_argument("--features", nargs='+', type= int, help="Array of feature indexes to capture from Tstat CSV generated file, separated by ' '.")
    parser.add_argument("--protocol", type=str, default= "tcp", help="Protocol to process; one of the following: [tcp, udp] (udp not implemented yet).")
    parser.add_argument("--version", type=str, default= "v3_3_A", help="Version of data_aggregator; one of the following: [v1, v1_point_5, v2, v3_1_A, v3_1_B, v3_2_A, v3_2_B, v3_3_A, v3_3_B].")
    parser.add_argument("--window-size", type=int, default= 5, help="Window size. Not used in versions 1, 3.1 and 3.2")
    parser.add_argument("--interval", type=int, default= 0.5, help="Time interval in seconds for time series construction. Not used in versions 1 and 1.5")
    parser.add_argument("--num-connections", type=int, default= 1000, help="Number of connections sent every interval of the time series. Not used in versions 1, 1.5 and 2")
    parser.add_argument("--timeout", type=float, default= 2.5, help="Timeout to send connections that have not filled a window. Only used in version 2")
    parser.add_argument("--mode", type=str, default= "static", help="Controls the way time intervals are calculated; one of the following: [static, streaming].")
    parser.add_argument("--monitored-device", type=str, default= "ceos2", help="Device to be monitored by Tstat.")
    parser.add_argument("--interface", type=str, default= "eth4", help="Interface of monitored device to be monitored by Tstat.")

    args = parser.parse_args()
    main(args)

    sys.exit(0)
