package com.cecloud.rocketmq.common.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

import java.util.concurrent.*;

public class ProducerFactory {
    private static volatile TransactionMQProducer transactionProducer = null;

    private static volatile DefaultMQProducer defaultMQProducer = null;

    /**
     * 构造一个事务类型的Producer并返回
     *
     * @return  事务类型
     */
    private static TransactionMQProducer initTransactionMQProducer() {
        try {
            // 指定事务的监听类
            TransactionListener transactionListener = new TransactionListenerImpl();
            TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");

            // 事务的check线程池，当调用回调方法 #initCommonMQProducer 时，将会使用此线程池
            // 一般情况下，如果业务事务一次性可以执行成功，那么不会走到check方法
            // 回调线程池可根据业务场景，自行配置其大小
            ExecutorService executorService = new ThreadPoolExecutor(
                    2,
                    5,
                    100, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(2000), r -> {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            });

            producer.setNamesrvAddr("localhost:9876");
            producer.setExecutorService(executorService);
            producer.setTransactionListener(transactionListener);
            producer.start();
            return producer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构造一个普通类型的Producer并返回
     *
     * @return  普通类型
     */
    private static DefaultMQProducer initCommonMQProducer() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            return producer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 单例，获取producer引用
     */
    public static TransactionMQProducer getTransactionProducer() {
        if (transactionProducer == null) {
            synchronized (ProducerFactory.class) {
                if (transactionProducer == null) {
                    transactionProducer = initTransactionMQProducer();
                }
            }
        }
        return transactionProducer;
    }

    public static DefaultMQProducer getCommonProducer() {
        if (defaultMQProducer == null) {
            synchronized (ProducerFactory.class) {
                if (defaultMQProducer == null) {
                    defaultMQProducer = initCommonMQProducer();
                }
            }
        }
        return defaultMQProducer;
    }

}
