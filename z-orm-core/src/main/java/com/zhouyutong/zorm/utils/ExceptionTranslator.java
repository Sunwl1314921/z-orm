package com.zhouyutong.zorm.utils;

import com.zhouyutong.zapplication.exception.*;
import com.zhouyutong.zorm.enums.DialectEnum;

/**
 * dao层异常转换器
 *
 * @Author zhouyutong
 * @Date 2017/6/20
 */
public class ExceptionTranslator {

    private ExceptionTranslator() {
    }

    public static RemoteCallException translate(Throwable ex, DialectEnum dialectEnum) {
        if (DialectEnum.ORACLE.equals(dialectEnum)) {
            return new OracleCallException(ex.getMessage(), ex);
        }
        if (DialectEnum.MYSQL.equals(dialectEnum)) {
            return new MysqlCallException(ex.getMessage(), ex);
        }
        if (DialectEnum.ELASTICSEARCH.equals(dialectEnum)) {
            return new ElasticsearchCallException(ex.getMessage(), ex);
        }
        if (DialectEnum.CASSANDRA.equals(dialectEnum)) {
            return new CassandraCallException(ex.getMessage(), ex);
        }
        return new RemoteCallException(ex.getMessage(), ex);
    }

    public static RemoteCallException translate(Throwable ex) {
        if (ex instanceof RemoteCallException) {
            return (RemoteCallException) ex;
        }
        return new RemoteCallException(ex.getMessage(), ex);
    }
}
