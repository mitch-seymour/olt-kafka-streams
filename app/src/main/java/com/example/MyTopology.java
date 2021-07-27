package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // add a source processor that reads data from the "tweets" topic
    KStream<byte[], String> stream =
        builder.stream("tweets", Consumed.with(Serdes.ByteArray(), Serdes.String()));

    KStream<byte[], String> filtered =
        stream.filter((key, value) -> value.toLowerCase().contains("bitcoin"));

    // print the output
    filtered.print(Printed.<byte[], String>toSysOut().withLabel("filter-tweets-stream"));

    return builder.build();
  }
}
