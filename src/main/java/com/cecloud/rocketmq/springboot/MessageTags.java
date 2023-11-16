package com.cecloud.rocketmq.springboot;

public enum MessageTags {
    COMMIT_TAG("commit"),
    ROLLBACK_TAG("rollback"),
    CHECK_BACK_AND_COMMIT_TAG("check_back_and_commit"),
    CHECK_BACK_AND_ROLLBACK_TAG("check_back_and_rollback"),
    UNKNOWN_TAG("unknown");

    private final String value;

    MessageTags(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
