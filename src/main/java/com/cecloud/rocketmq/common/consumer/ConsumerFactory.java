package com.cecloud.rocketmq.common.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class ConsumerFactory {

    /**
     * 构造一个事务类型的Producer并返回
     *
     * @return  事务类型
     */
    public static void startConsumer() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_consumer_name");
            consumer.subscribe("TopicTest", "*");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setMaxReconsumeTimes(3);
            consumer.registerMessageListener(new MessageListenerConcurrentlyImpl());
            consumer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
