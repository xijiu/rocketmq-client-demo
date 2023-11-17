package com.cecloud.rocketmq.springboot.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;


@Component
@Slf4j
@RocketMQMessageListener(consumerGroup = "transaction-consumer-group", topic = "transaction-message-topic")
public class TransactionConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String msg) {
        log.info("消费组接收到事务消息: {}", msg);
    }
}
