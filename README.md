# GMS
General Monitoring System

```
# please refer to https://fede1024.github.io/rust-rdkafka/rdkafka/index.html#installation
# for rdkafka
sudo apt-get install cmake 
```


## test in local environment

```
# run kafka on local machine simply.
docker run -p 9092:9092 --name test_cnc_kafka apache/kafka:3.7.0
# execute on the other terminal
docker exec -it test_cnc_kafka /opt/kafka/bin/kafka-topics.sh --create --topic alert-infos --bootstrap-server localhost:9092
# if you want to see messages on kafka
docker exec -it test_cnc_kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alert-infos --from-beginning --bootstrap-server localhost:9092
```

```
# run harvester
cargo run -- harvester --config-dir ./resources/sample
```

```
# run refinery
cargo run -- refinery --config-dir ./resources/sample
```



# configuration

## checker.toml

- It is not allowed for multiple checkers to consume the same kafka topic.

