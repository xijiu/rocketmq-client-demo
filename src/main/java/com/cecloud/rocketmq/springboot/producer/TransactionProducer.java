package com.cecloud.rocketmq.springboot.producer;

import com.cecloud.rocketmq.PubTools;
import com.cecloud.rocketmq.springboot.TransactionLogManager;
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
import java.util.List;

@Component
@Slf4j
public class TransactionProducer {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;


    @Value("${rocketmq.consumer.topic}")
    private String topic;

    public void sendTransactionalMessage() throws InterruptedException {
        List<Message<String>> messages = buildMessages();
        for (Message<String> message : messages) {
            TransactionSendResult transactionSendResult = rocketMQTemplate.sendMessageInTransaction(topic, message, null);
            Thread.sleep(100);
            if (transactionSendResult.getSendStatus() == SendStatus.SEND_OK) {
                log.info("发送事务消息成功!消息ID为:{}, 消息tag为: {}", transactionSendResult.getMsgId(), message.getHeaders().get("tag"));
            }
        }
    }

    private List<Message<String>> buildMessages() {
        String[] tags = {"tag1", "tag2", "tag3", "tag4", "tag5"};
        List<Message<String>> res = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String body = "This is a transactional order created at " + PubTools.now() + ", transactionId编号为： trans_id_" + (i + 1);
            Message<String> message = MessageBuilder.withPayload(body)
                    .setHeader("transactionId", "trans_id_" + (i + 1))
                    .setHeader("tag", tags[i % tags.length])
                    .build();
            res.add(message);
        }
        return res;
    }

}
