package com.github.sailendra.bank.transaction;

import java.time.Instant;

public class CustomerTransaction {

    private String name;
    private int transAmount;
    private String timeStamp;

    public CustomerTransaction(String name) {
        this.name = name;

        //generates a random number in range of 1-999
        this.transAmount = (int) (Math.random() * 1000 + 1);

        //generates string representation of current datetime
        this.timeStamp = Instant.now().toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTransAmount() {
        return transAmount;
    }

    public void setTransAmount(int transAmount) {
        this.transAmount = transAmount;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "BankCustomerTransaction{" +
                "name='" + name + '\'' +
                ", transAmount=" + transAmount +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }
}
