package com.cecloud.rocketmq.ons;

import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionProducer;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;

import java.util.Properties;

public class ONSTransactionProducer {
    private static volatile TransactionProducer transactionProducer;

    public static void main(String[] args) {
        Properties properties = initProperties();
        initTransactionProducer(properties);
        sendMsg();

    }

    /**
     * 事务消息发送示例
     */
    private static void sendMsg() {
        for (int i = 0; i < 3; i++) {
            Message msg = new Message("test_topic","tag_name", ("messageBody ====>" + i).getBytes());
            SendResult sendResult = transactionProducer.send(msg, new LocalTransactionExecuter() {
                /**
                 * 事务本身的逻辑需要写在这里
                 *
                 * @param msg 消息
                 * @param arg 应用自定义参数，由send方法传入并回调
                 * @return  事务状态，如果返回UnKnow，后续将会回调{@link LocalTransactionCheckerImpl#check(Message)}
                 */
                @Override
                public TransactionStatus execute(Message msg, Object arg) {
                    System.out.println("do local transaction");
                    return TransactionStatus.CommitTransaction;
                }
            }, null);
            System.out.println(JSON.toJSONString(sendResult));
        }
    }

    /**
     * 初始化一个事务producer，并指明将来的checker实现类
     *
     * @param properties    属性
     */
    private static void initTransactionProducer(Properties properties) {
        LocalTransactionCheckerImpl localTransactionChecker = new LocalTransactionCheckerImpl();
        transactionProducer = ONSFactory.createTransactionProducer(properties, localTransactionChecker);
        transactionProducer.start();
    }

    /**
     * 初始化属性
     *
     * @return  相关属性
     */
    private static Properties initProperties() {
        Properties properties = new Properties();
        // 事务消息的Group ID
        properties.put(PropertyKeyConst.GROUP_ID, "XXX");
        // 实例用户名
        properties.put(PropertyKeyConst.AccessKey,"INSTANCE USER NAME");
        // 实例密码
        properties.put(PropertyKeyConst.SecretKey, "INSTANCE PASSWORD");
        // NAMESRV地址
        properties.put(PropertyKeyConst.NAMESRV_ADDR, "ACCESS POINT");
        return properties;
    }
}
