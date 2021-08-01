package com.example.step5;

import com.example.util.TweetParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

public class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // read the tweets topic as a stream
    KStream<String, String> tweetStream =
        builder.stream("tweets", Consumed.with(Serdes.String(), Serdes.String()));

    // read the crypto-symbols topic as a table
    KTable<String, String> symbolsTable =
        builder.table("crypto-symbols", Consumed.with(Serdes.String(), Serdes.String()));

    // rekey the tweets by currency
    KStream<String, String> tweetsRekeyed = tweetStream.selectKey(TweetParser::getCurrency);

    // join
    KStream<String, String> joined =
        tweetsRekeyed.join(
            symbolsTable, (tweet, symbol) -> String.format("%s - (%s)", tweet, symbol));

    // count
    KTable<String, Long> counts = tweetsRekeyed.groupByKey().count();

    // print
    counts.toStream().print(Printed.<String, Long>toSysOut().withLabel("counts"));

    return builder.build();
  }
}