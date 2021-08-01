package com.example;

import com.example.util.TweetParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

public class MyTopology {

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    // read the tweets topic as a stream
    KStream<String, String> tweetStream =
        builder.stream("tweets", Consumed.with(Serdes.String(), Serdes.String()));

    // print
    tweetStream.print(Printed.<String, String>toSysOut().withLabel("tweets"));

    // read the crypto-symbols topic as a table
    KTable<String, String> symbolsTable =
        builder.table("crypto-symbols", Consumed.with(Serdes.String(), Serdes.String()));

    // print
    symbolsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("crypto-symbols"));

    // rekey the tweets by currency
    KStream<String, String> tweetsRekeyed =
        tweetStream.flatMap(
            (key, value) -> {
              List<String> currencies = TweetParser.getCurrencies(value);
              List<KeyValue<String, String>> records = new ArrayList<>();
              for (String currency : currencies) {
                records.add(KeyValue.pair(currency, value));
              }
              return records;
            });

    // print
    tweetsRekeyed.print(Printed.<String, String>toSysOut().withLabel("tweets-rekeyed"));

    // join
    KStream<String, String> joined =
        tweetsRekeyed.join(
            symbolsTable, (tweet, symbol) -> String.format("%s - (%s)", tweet, symbol));

    // print
    joined.print(Printed.<String, String>toSysOut().withLabel("joined"));

    // count
    KTable<String, Long> counts = tweetsRekeyed.groupByKey().count();

    // print
    counts.toStream().print(Printed.<String, Long>toSysOut().withLabel("counts"));

    // count materialized
    KTable<String, Long> countsM =
        tweetsRekeyed
            .groupByKey()
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

    return builder.build();
  }
}
