package com.github.sailendra.bank.transaction;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class BankTransactionProducer {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "bank-transactions";
    private static final List<String> CUSTOMERS = Arrays.asList("ADAM", "ROBERT", "STEVE", "JENNY", "MATT", "PETER");

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(BankTransactionProducer.class);

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }));

        int batchId = 1;
        while (true) {
            logger.info("Producing batch: " + batchId);
            try {
                producer.send(generateTransaction(pickRandomCustomer()));
                Thread.sleep(100);

                producer.send(generateTransaction(pickRandomCustomer()));
                Thread.sleep(100);

                producer.send(generateTransaction(pickRandomCustomer()));
                Thread.sleep(100);

                batchId += 1;
            } catch (InterruptedException ex) {
                break;
            }
        }

        producer.flush();
        producer.close();
    }

    private static String pickRandomCustomer() {
        return CUSTOMERS.get(new Random().nextInt(CUSTOMERS.size()));
    }

    public static ProducerRecord<String, String> generateTransaction(String customerName) {
        CustomerTransaction transaction = new CustomerTransaction(customerName);
        return new ProducerRecord<>(TOPIC_NAME, customerName, buildJSONForTransaction(transaction));
    }

    private static String buildJSONForTransaction(final CustomerTransaction transaction) {
        ObjectNode jsonObj = JsonNodeFactory.instance.objectNode();

        jsonObj.put("name", transaction.getName());
        jsonObj.put("amount", transaction.getTransAmount());
        jsonObj.put("time", transaction.getTimeStamp());

        return jsonObj.toString();
    }
}
