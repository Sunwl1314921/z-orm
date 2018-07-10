package com.zhouyutong.zorm.dao.elasticsearch.annotation;

import java.lang.annotation.*;

/**
 * 标注entity对应的es文档描述
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Document {
    //对应的es的索引名称
    String indexName();

    /**
     * 索引名模式
     * 支持动态索引,如果不为""，indexName就是前缀
     * 支持日期模式 date{yyyy-mm-dd}
     */
    String indexNamePattern() default "";

    //对应的es索引的类型名称
    String typeName();
}
