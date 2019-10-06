package com.github.sailendra.bank.transaction.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sailendra.bank.transaction.BankTransactionProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BankTransactionProducerTests {

    @Test
    public void generateTransactionTest() {
        ProducerRecord<String, String> record = BankTransactionProducer.generateTransaction("STEVE");
        String key = record.key();
        String value = record.value();

        Assert.assertEquals(key, "STEVE");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            Assert.assertEquals(node.get("name").asText(), "STEVE");
            Assert.assertTrue("Amount should be less than 1000", node.get("amount").asInt() < 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
