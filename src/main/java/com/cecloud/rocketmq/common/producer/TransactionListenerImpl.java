package com.cecloud.rocketmq.common.producer;

import com.cecloud.rocketmq.PubTools;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * rocketmq事务的监听类
 */
public class TransactionListenerImpl implements TransactionListener {

    /**
     * 在这个方法中编写事务逻辑，建议使用try-catch块
     * {@link LocalTransactionState#COMMIT_MESSAGE} ： 当本地事务成功执行后返回此状态
     * {@link LocalTransactionState#ROLLBACK_MESSAGE} ： 当本地事务执行失败，需要回滚，那么返回此状态
     * {@link LocalTransactionState#UNKNOW} ：   当本地事务状态未知，或事务执行时间较长，无法判断其最终状态，
     *                                          那么此时可返回此状态，后续broker还会进行回调
     *
     * @param msg   消息体
     * @param arg   参数，可忽略
     * @return  事务状态
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        try {
            System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", executeLocalTransaction ");
            // 模拟事物耗时
            PubTools.sleep(10);
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            transactionRollBack(msg);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

    /**
     * 事务异常，需要执行事务回滚或者事务补偿等操作
     *
     * @param msg   消息体
     */
    private void transactionRollBack(Message msg) {
        System.out.println(msg);
        // do something
    }

    /**
     * 当某个事物操作在第一次不成功时，即事务状态不是{@link LocalTransactionState#COMMIT_MESSAGE}时，
     * Broker会在合适的时机多次回调此方法
     *
     * @param msg   消息体
     * @return  事务状态
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", checkLocalTransaction ");
        if (Math.random() > 0.4) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (Math.random() > 0.2) {
            return LocalTransactionState.UNKNOW;
        } else {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }
}
