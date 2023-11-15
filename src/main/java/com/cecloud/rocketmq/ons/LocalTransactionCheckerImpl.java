package com.cecloud.rocketmq.ons;

import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;

/**
 * 该类主要用于接受事务回查
 * 只有当事务状态为{@link TransactionStatus#Unknow}时，才有可能回调此方法
 */
public class LocalTransactionCheckerImpl implements LocalTransactionChecker {
   
    @Override
    public TransactionStatus check(Message msg) {
        System.out.println("receive transaction msg: " + JSON.toJSONString(msg));
        if (Math.random() > 0.4) {
            return TransactionStatus.CommitTransaction;
        } else if (Math.random() > 0.2) {
            return TransactionStatus.Unknow;
        } else {
            return TransactionStatus.RollbackTransaction;
        }
    }
}