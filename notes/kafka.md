
Create topic
```sh
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic perf_test
```

Produce records for a given topic.
```sh
kafka-producer-perf-test --topic perf_test --num-records 5 --record-size 100 --producer-props bootstrap.servers=$BOOTSTRAP_SERVER --throughput -1
```

Consume records for a given topic.
```sh
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic perf_test
```

Consumer perf test
```sh
kafka-consumer-perf-test --bootstrap-server 127.0.0.1:9092 --topic perf-test --messages 1000
```