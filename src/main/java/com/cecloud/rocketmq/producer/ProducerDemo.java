package com.cecloud.rocketmq.producer;

import com.alibaba.fastjson.JSON;
import com.cecloud.rocketmq.PubTools;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * producer demo入口
 */
public class ProducerDemo {

    public static void main(String[] args) throws Exception {
        // 发送事务消息
        sendTransactionMsg();

        // 发送普通消息
        sendCommonMsg();
    }


    /**
     * 发送事务消息的demo，这里相对比较简单，只是触发了一下事务消息的发送，主要事务逻辑需要参考{@link TransactionListenerImpl}
     *
     * @throws Exception    各类异常
     */
    private static void sendTransactionMsg() throws Exception {
        TransactionMQProducer producer = ProducerFactory.getTransactionProducer();
        for (int i = 0; i < 10; i++) {
            Message msg = initMsg(i);
            System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", prepare send");
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", result is " + sendResult);
        }
    }

    /**
     * 普通消息的发送，如果当前消息不是事务消息，那么参照此方法代码
     *
     * @throws Exception    各类异常
     */
    private static void sendCommonMsg() throws Exception {
        DefaultMQProducer producer = ProducerFactory.getCommonProducer();
        for (int i = 0; i < 10; i++) {
            Message msg = initMsg(i);
            SendResult sendResult = producer.send(msg);
            System.out.println(JSON.toJSONString(sendResult));
        }
    }

    private static Message initMsg(int index) throws Exception {
        return new Message("TopicTest", "", "key" + index,
                ("Hello RocketMQ " + index).getBytes(RemotingHelper.DEFAULT_CHARSET));
    }
}