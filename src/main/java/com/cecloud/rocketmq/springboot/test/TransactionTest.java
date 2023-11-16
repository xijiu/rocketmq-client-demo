package com.cecloud.rocketmq.springboot.test;

import com.cecloud.rocketmq.springboot.TransactionLogManager;
import com.cecloud.rocketmq.springboot.producer.TransactionProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TransactionTest {

    @Autowired
    private TransactionProducer producer;

    @Autowired
    private TransactionLogManager logManager;


    @Test
    public void transactionMessage() throws InterruptedException {
        producer.sendTransactionalMessage();
        System.out.println(logManager.list());
    }

}