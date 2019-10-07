package com.github.psydoc.kafka.fav;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorStreamApp {
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE); //built with kafka streams 2.3.0

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: We create the topic of users keys to colours
        KStream<String, String> textLines = builder.stream("favourite-colour-input");

        KStream<String, String> usersAndColours = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired colours (could be a data sanitization step
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        usersAndColours.to("user-keys-and-colours");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colours");

        // step 3 - we count the occurrences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // 6 - we output the results to a Kafka Topic
        favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

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
