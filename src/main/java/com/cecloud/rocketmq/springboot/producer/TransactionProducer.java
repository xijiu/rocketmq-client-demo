package com.cecloud.rocketmq.springboot.producer;

import com.cecloud.rocketmq.springboot.MessageTags;
import com.cecloud.rocketmq.PubTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
@Slf4j
public class TransactionProducer {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

//    @Autowired
//    private MyTransactionListener myTransactionListener;

    @Value("${rocketmq.consumer.topic}")
    private String topic;

    public void sendTransactionalMessage() throws InterruptedException {
//        ((TransactionMQProducer) rocketMQTemplate.getProducer()).setTransactionListener((TransactionListener) myTransactionListener);
        List<Message<String>> messages = buildMessages();
        for (Message<String> message : messages) {
            TransactionSendResult transactionSendResult = rocketMQTemplate.sendMessageInTransaction(topic, message, null);
            Thread.sleep(10);
            if (transactionSendResult.getSendStatus() == SendStatus.SEND_OK) {
                log.info("发送事务消息成功!消息ID为:{}", transactionSendResult.getMsgId());
            }
        }
    }

    private List<Message<String>> buildMessages() {
        List<MessageTags> tags = new ArrayList<>(Arrays.asList(MessageTags.values()));
        List<Message<String>> res = new ArrayList<>(100);
        for (int i = 0; i < 10; i++) {
            Message<String> message = MessageBuilder.withPayload("This is a transactional order create at" + PubTools.now())
                    .setHeader("transactionId", "trans_id_" + i)
                    .setHeader("tag", tags.get(i % tags.size()))
                    .build();
            res.add(message);
        }
        return res;
    }

}
