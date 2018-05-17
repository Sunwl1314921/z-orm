package com.zhouyutong.zorm.dao.cassandra.annotation;

import java.lang.annotation.*;

/**
 * 标注entity对应的表名
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CassandraTable {
    /**
     * 键空间
     * @return
     */
    String keyspace();
    String tableName();
}
