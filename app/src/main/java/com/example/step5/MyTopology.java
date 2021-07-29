package com.example.step5;

import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

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

    // filter
    KStream<byte[], String> filteredTweets =
        lowercaseTweets.filter(
            (key, value) ->
                value.contains("dogecoin")
                    || value.contains("bitcoin")
                    || value.contains("ethereum"));

    // branch
    Map<String, KStream<byte[], String>> branches =
        filteredTweets
            .split(Named.as("branch-"))
            .branch(
                (key, value) -> value.contains("dogecoin"), /* first predicate  */
                Branched.as("dogecoin"))
            .defaultBranch(Branched.as("default"));

    return builder.build();
  }
}
