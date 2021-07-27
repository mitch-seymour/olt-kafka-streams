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

    // read from the `tweets` topic
    KStream<byte[], String> tweets =
        builder.stream("tweets", Consumed.with(Serdes.ByteArray(), Serdes.String()));

    // print the contents of each stream for debugging purposes
    tweets.print(Printed.<byte[], String>toSysOut().withLabel("tweets"));

    return builder.build();
  }
}
