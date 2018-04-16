package com.zhouyutong.zorm.enums;

/**
 * DB名称枚举
 *
 * @author zhouyutong
 */
public enum DialectEnum {
    MYSQL("mysql"),
    ORACLE("oracle"),
    ELASTICSEARCH("elasticsearch"),
    CASSANDRA("cassandra");

    private final String value;

    DialectEnum(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
