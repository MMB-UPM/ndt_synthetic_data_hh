cd /
echo "Launching Tstat service"

sleep 5
app/bin/tstat -s piped "/app/pcaps/ceos2_eth4_rev3.pcap" >> /dev/null&
#/app/bin/tstat -l -i eno1 -s piped >> /dev/null &
TSTAT_PID=`echo $!`

# Wait for tstat to create the necessary directories
echo "Waiting for Tstat to generate the output directories"
sleep 10
output_dir=$(ls -t /piped | head -1)
TSTAT_PATH="/piped/$output_dir"
echo "TSTAT_PATH: $TSTAT_PATH"

while [ ! -f "$TSTAT_PATH/log_tcp_temp_complete" ]
do
	echo "Waiting for 2 seconds"
	sleep 2 # or less like 0.2
done

echo $TSTAT_PATH
echo "Starting Data Aggregator"

cd /app

#add args here
python3 . --features 4 5 6 7 8 11 18 19 21 30 33 34 70 75 95 113 114 --ignore-first --version $1 --tstat-dir $TSTAT_PATH
echo "All the process are running"
