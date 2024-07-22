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
docker exec -it test_cnc_kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alert-infos --bootstrap-server localhost:9092
```

```
# run harvester
RUST_LOG=debug cargo run -- harvester --config-dir ./configs
```

```
# run refinery
RUST_LOG=debug cargo run -- refinery --config-dir ./configs
```

```
# run config_updater
RUST_LOG=debug cargo run -- config_updater --config-dir ./configs
```



# configuration

## checker_info.toml

- It is not allowed for multiple checkers to consume the same kafka topic.

