KAFKA_PORT=9094

#echo "Starting traffic replay"
#sh -c 'nohup sudo tcpreplay-edit --stats=1 --enet-vlan=del --fixcsum --enet-smac="fa:16:3e:03:c4:15","fa:16:3e:7d:46:9d" -i eno1 capture+crypto+type2+8+vlan+filtered+2.pcap > /dev/null 2>&1 &'

#echo ""

echo "Starting data aggregator"
docker run -d --name=data_aggregator -p $KAFKA_PORT:$KAFKA_PORT --network=host -e VERSION=$1 data_aggregator .

