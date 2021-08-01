package com.example.step3;

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

    return builder.build();
  }
}
