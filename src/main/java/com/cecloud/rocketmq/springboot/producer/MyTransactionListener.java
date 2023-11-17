package com.cecloud.rocketmq.springboot.producer;

import com.cecloud.rocketmq.springboot.TransactionLog;
import com.cecloud.rocketmq.springboot.TransactionLogManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;


/**
 * rocketmq事务的监听类
 */
@Slf4j
@RocketMQTransactionListener(rocketMQTemplateBeanName = "rocketMQTemplate")
public class MyTransactionListener implements RocketMQLocalTransactionListener {

    @Autowired
    private TransactionLogManager transactionLogManager;

    /**
     * half消息发送成功后回调此方法，执行本地事务
     *
     * 在这个方法中编写事务逻辑，建议使用try-catch块
     * {@link LocalTransactionState#COMMIT_MESSAGE} ： 当本地事务成功执行后返回此状态
     * {@link LocalTransactionState#ROLLBACK_MESSAGE} ： 当本地事务执行失败，需要回滚，那么返回此状态
     * {@link LocalTransactionState#UNKNOW} ：   当本地事务状态未知，或事务执行时间较长，无法判断其最终状态，
     *                                          那么此时可返回此状态，后续broker还会进行回调
     *
     * @param message   消息体
     * @param arg   参数，可忽略
     * @return  事务状态
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        String transactionId = (String) message.getHeaders().get("transactionId");
        String tag = (String) message.getHeaders().get("tag");
        log.info("执行本地事务, 事务Id：{}, tag: {}", transactionId, tag);
        if (tag.equals("tag1")) {
            String body = message.getPayload().toString();
            localTransactionCreateOrder(transactionId, body);
            log.info("已提交本地事务: {}",transactionId);
            return RocketMQLocalTransactionState.COMMIT;
        } else if (tag.equals("tag2")) {
            log.error("本地事务执行失败");
            transactionRollBack(message);
            return RocketMQLocalTransactionState.ROLLBACK;
        } else {
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    /**
     * 当某个事物操作在第一次不成功时，即事务状态不是{@link LocalTransactionState#COMMIT_MESSAGE}时，
     * Broker会在合适的时机多次回调此方法
     *
     * @param msg   消息体
     * @return  事务状态
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        String tag = (String) msg.getHeaders().get("tag");
        log.info("回查本地事务状态, 事务id: {}, tag: {}", msg.getHeaders().get("transactionId"), tag);
        RocketMQLocalTransactionState state;
        if (tag.equals("tag3")) {
            state = RocketMQLocalTransactionState.COMMIT;
        }
        else if (tag.equals("tag4")) {
            state = RocketMQLocalTransactionState.ROLLBACK;
        }
        else {
            state = RocketMQLocalTransactionState.UNKNOWN;
        }
        return state;
    }

    private void localTransactionCreateOrder(String transactionId, String body) {
        // 把本地事务存入日志
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setDetail(body);
        transactionLogManager.insert(transactionLog);
        log.info("创建订单成功，订单id: {}, 订单内容: {}",  transactionLog.getId(), transactionLog.getDetail());
    }

    /**
     * 事务异常，需要执行事务回滚或者事务补偿等操作
     *
     * @param message   消息体
     */
    private void transactionRollBack(Message message) {
        // 撤销订单
        String transactionId = (String) message.getHeaders().get("transactionId");
        transactionLogManager.remove(transactionId);
        log.info("本地事务回滚，删除对应订单, 订单id: {}, 订单内容: {}",  transactionId, message.getPayload());
    }
}
