# How to setup environment
1. download [Docker Desktop](https://www.docker.com/). 
2. download docker-compose.ymal 
3. Open Docker Desktop (So the Docker daemon will start running)
4. open terminal, and go to the location where you put the docker-compose.ymal file    
```
docker compose up -d
```
This command line will build the containers that following the setup information from this docker-compose.ymal file. You only need to do this once in your own laptop.  
Now you have:

| container  | quantity |port|
| ------------- | ------------- |---|
|Zookeeper|1|2181|
|kafka|3|9092, 9093, 9094|
|kafdrop|1|9000|
|redis|1|6379|

    - kafka-1 on port 29092 between containers, localhost:9092 on host machine (use this as a producer to generate mock data)
    - kafka-2 on port 29093 between containers, localhost:9093 on host machine
    - kafka-3 on port 29094 between containers, localhost:9094 on host machine


### Now use http://localhost:9000/ to monitor all brokers and topics
![image](https://user-images.githubusercontent.com/14934562/220826742-146f47a3-fc4b-4201-97cc-5ada930be912.png)


### To use the data generating script, exec container with root permission
```
docker exec -it -u root kafka-1 bash
whoami //root
```


### Create a topic (topic name = ODS_BASE_LOG, --partitions = 3 ~ 6)
```
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic ODS_BASE_LOG &&
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic DWD_PAGE_LOG_TOPIC &&
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic DWD_PAGE_DISPLAY_TOPIC &&
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic DWD_PAGE_ACTION_TOPIC &&
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic DWD_START_LOG_TOPIC &&
kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --replication-factor 1 --partitions 4 --topic DWD_ERROR_LOG_TOPIC
```
### List all topics
```
kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 --list
```

### Describe a topic
```
kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 --describe --topic ODS_BASE_LOG
```

### Delete topic
```
kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 --delete --topic ODS_BASE_LOG
```

# Now we can start using the consumer/producer

### Consumer

make a consle consumer listen to the comming data
```
kafka-console-consumer --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 --from-beginning --topic ODS_BASE_LOG
```

### Producer

1. create input by yourself
```
kafka-console-producer --broker-list kafka-1:29092,kafka-2:29093,kafka-3:29094 --topic ODS_BASE_LOG
> hi this is a testing message //consumer console should show the same message
```
2. produce with the script
```
./mock_data/log.sh TODAYS_DATE | kafka-console-producer --broker-list kafka-1:29092,kafka-2:29093,kafka-3:29094 --topic ODS_BASE_LOG > /dev/null
```
