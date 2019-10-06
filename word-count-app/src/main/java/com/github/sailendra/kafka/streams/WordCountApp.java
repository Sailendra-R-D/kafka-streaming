package com.github.sailendra.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE); //built with kafka streams 2.3.0

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2 - map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
                // can be alternatively written as :
                // .mapValues(String::toLowerCase)
                // 3 - flatmap values split by space
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split("\\W+")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, value) -> value)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurrences
                .count();

        wordCounts.toStream().to("word-count-output");

        Topology topology = builder.build(config);
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        // printing the topology
        TopologyDescription description = topology.describe();
        System.out.println(description);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
