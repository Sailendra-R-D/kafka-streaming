package com.github.sailendra.kafka.streams.test;

import com.github.sailendra.kafka.streams.WordCountApp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class WordCountAppTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();

    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    String appId = "test_app";

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver() {
        try {
            testDriver.close();
        } catch (Exception e) {
            //there is a bug on Windows that does not delete the state directory properly.
            // In order for the test to pass, the directory must be deleted manually
            try {
                Files.walk(Paths.get(String.format("C:\\tmp\\kafka-streams\\%s", appId)))
                        .map(Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);
            } catch (IOException ex) {
            }
        }
    }

    public void pushNewInputRecord(String value) {
        testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
    }

    @Test
    public void dummyTest() {
        String dummy = "Du" + "mmy";
        Assert.assertEquals(dummy, "Dummy");
    }

    public ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void makeSureCountsAreCorrect() {
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);
        Assert.assertEquals(readOutput(), null);

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);

    }

    @Test
    public void makeSureWordsBecomeLowercase() {
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);

    }
}
