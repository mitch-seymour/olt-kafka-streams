package com.example.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TweetParser {

  public static List<String> getCurrencies(String tweetText) {
    List<String> currencies = Arrays.asList("dogecoin", "bitcoin", "ethereum");
    List<String> words =
        Arrays.asList(tweetText.replaceAll("[^a-zA-Z ]", "").toLowerCase().trim().split(" "));
    return words.stream().distinct().filter(currencies::contains).collect(Collectors.toList());
  }

  public static String getCurrency(String key, String tweetText) {
    List<String> currencies = getCurrencies(tweetText);
    if (currencies.size() > 0) {
      return currencies.get(0);
    }
    return "";
  }
}
