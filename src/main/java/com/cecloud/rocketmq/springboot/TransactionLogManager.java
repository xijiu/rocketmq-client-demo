package com.cecloud.rocketmq.springboot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class TransactionLogManager {
    private static Map<String, TransactionLog> orderMap;

    static {
        orderMap = new HashMap<>(100);
    }

    public void insert(TransactionLog log) {
        orderMap.put(log.getId(), log);
    }

    public void remove(String id) {
        orderMap.remove(id);
    }

    public boolean contains(String id) {
        return orderMap.containsKey(id);
    }

    public void list() {
        log.info("当前订单库状态:");
        for (String key : orderMap.keySet()) {
            log.info("事务id: {}, 订单内容: {}", key, orderMap.get(key).getDetail());
        }
    }
}
