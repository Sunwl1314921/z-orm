package com.zhouyutong.zorm.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * 封装基本的Query
 *
 * @author zhouyutong
 * @non-threadsafe 线程不安全对象，建议只用作方法内部变量使用
 */
@Getter
@ToString
public class Query {
    /**
     * 一次查询中的条件
     */
    private Criteria criteria;
    /**
     * 一次查询中需要返回的字段,如果不设置返回所有
     */
    private final List<String> fields = Lists.newArrayList();
    /**
     * 一次查询中GroupBy
     */
    private final List<GroupBy> groupBys = Lists.newArrayList();
    /**
     * 一次查询中order by
     */
    private final List<OrderBy> orderBys = Lists.newArrayList();
    private int offset;
    private int limit;
    private String hint;

    private Query() {
    }

    private Query(Criteria criteria) {
        this.criteria = criteria;
    }

    public static Query query() {
        return new Query();
    }

    public static Query query(Criteria criteria) {
        return new Query(criteria);
    }

    public Query criteria(Criteria criteria) {
        this.criteria = criteria;
        return this;
    }

    public Query orderBy(OrderBy... orderByArr) {
        for (OrderBy orderBy : orderByArr) {
            orderBys.add(orderBy);
        }
        return this;
    }

    public Query groupBy(GroupBy... groupByArr) {
        for (GroupBy groupBy : groupByArr) {
            groupBys.add(groupBy);
        }
        return this;
    }

    public Query includeField(String... fieldArr) {
        for (String field : fieldArr) {
            fields.add(field);
        }
        return this;
    }

    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }

    public Query hint(String hint) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hint), "Param key was %s, It must be not null or empty", hint);
        this.hint = hint;
        return this;
    }
}
