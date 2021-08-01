package com.example;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

class App {
  public static void main(String[] args) {
    Topology topology;
    String step = System.getenv().getOrDefault("step", "1");
    switch (step) {
      case "2":
        System.out.println("Running step 2 version");
        topology = com.example.step2.MyTopology.build();
        break;

      case "3":
        System.out.println("Running step 3 version");
        topology = com.example.step3.MyTopology.build();
        break;

      case "4":
        System.out.println("Running step 4 version");
        topology = com.example.step4.MyTopology.build();
        break;

      case "5":
        System.out.println("Running step 5 version");
        topology = com.example.step5.MyTopology.build();
        break;

      case "6":
        System.out.println("Running step 6 version");
        topology = com.example.step6.MyTopology.build();
        break;

      default:
        System.out.println("Running step 2 version");
        topology = com.example.step2.MyTopology.build();
    }

    // set the required properties for running Kafka Streams
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-" + step);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    // set some optional properties
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // disable DSL cache
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    // build the topology and start streaming!
    KafkaStreams streams = new KafkaStreams(topology, config);

    // close the Kafka Streams threads on shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    System.out.println("Starting Kafka Streams application");
    streams.start();

    // get key-value store
    // ReadOnlyKeyValueStore<String, Long> stateStore =
    //   streams.store(
    //        StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
  }
}
