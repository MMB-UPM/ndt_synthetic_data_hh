echo "Building Data Aggregator"

docker build -t data_aggregator -f ./data_aggregator/Dockerfile ./data_aggregator
