package com.example.step3;

import java.util.Arrays;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

public class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // read the topic as a stream
    KStream<byte[], String> tweetStream =
        builder.stream("tweets", Consumed.with(Serdes.ByteArray(), Serdes.String()));

    // sentences (1:N transform)
    KStream<byte[], String> sentences =
        tweetStream.flatMapValues((key, value) -> Arrays.asList(value.split("\\.")));

    // lowercase tweets (1:1 transform)
    KStream<byte[], String> lowercaseTweets =
        sentences.mapValues((key, value) -> value.toLowerCase().trim());

    // print the last step for debugging purposes
    lowercaseTweets.print(Printed.<byte[], String>toSysOut().withLabel("lowercase-tweets"));

    return builder.build();
  }
}
