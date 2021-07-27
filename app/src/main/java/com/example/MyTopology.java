package com.example;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // add processors here

    return builder.build();
  }
}
