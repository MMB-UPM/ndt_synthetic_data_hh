import os.path
import requests
import argparse
import json
import base64
import logging
from time import sleep
from sys import stdout

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

def send_data(url: str, path: str, filename: str, metadata: dict) -> None:
    """
    Uploads a file to a specified URL using a multipart/form-data POST request.
    Args:
        url (str): The URL to which the file will be uploaded.
        path (str): The path to the file to be uploaded.
        file_name (str): The name of the file to be uploaded.
        meta_data (dict): Strict usage. The dict must have one key "metadata" and its value is a json dumped string
        entailing a dict with "name", "params" and "cv_acc".
    Returns:
        None: Prints upload success or error messages.
    """
    # Prepare files
    with open(os.path.join(path, filename), "rb") as f:
        file_content = base64.b64encode(f.read()).decode("ascii")
        
    data = {'file_data': {'filename': filename, 'file_content': file_content, 'content_type': f'file/{filename[filename.index(".")+1:]}'}, 'metadata': metadata}
    headers = {"Content-Type": "application/json"}
    
    # Make the POST request
    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code == 201:
        LOGGER.info("File [%s] upload successful.", filename)
        LOGGER.info("Response content: %s", response.text)
        
    else:
        LOGGER.error("Error: %s - %s", response.status_code, response.text)

def main(args):
    """
    Main function for sending models and metadata back to the storage services.
    Args:
        args (Any): Command-line arguments and options.
    Returns:
        None
    """

    # Make output path, load the "best model" metadata  (SVM.csv) and process them as dicts to send to the server
    if args.filename.endswith("joblib"):
        meta_dict = json.load(open(os.path.join(args.path, f"{args.filename[:-6]}json"), "r"))
    elif args.filename.endswith("pkl"):
        meta_dict = json.load(open(os.path.join(args.path, f"{args.filename[:-3]}json"), "r"))
    else:
        meta_dict = {}
        
    LOGGER.info(meta_dict)

    # Send models and meta-data to the API
    api_url = f"{args.url}/{args.index}/_doc"
    LOGGER.info("API URL: %s", api_url)
    success = False
    while not success:
        try:
            send_data(url=api_url,
                      path=args.path,
                      filename=args.filename,
                      metadata=meta_dict)
            success = True
        except Exception:
            LOGGER.warning("Failed to connect to AI Model Repository. Retrying in 1 second...")
            sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train and validate linear models with k-fold cross-validation.")
    parser.add_argument("--index", type=str, default='models', help="Aggregator to use. Can be 'pfcpflowmeter-ai', 'tstat-ai', 'cicflowmeter-ai'. ")
    parser.add_argument("--url", type=str, default='http://localhost:9200', help="API URL to send models and metadata.")
    parser.add_argument("--path", type=str, default="./models/", help="Where to store the models that will be send to the ElasticSearch storage service.")
    parser.add_argument("--filename", type=str, default="model.pkl", help="Filename of file to send.")
    args = parser.parse_args()
    main(args)
