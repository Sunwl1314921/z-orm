package com.zhouyutong.zorm.dao.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zhouyutong.zorm.dao.cassandra.annotation.CassandraTable;
import com.zhouyutong.zorm.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @Author zhouyutong
 * @Date 2017/5/11
 */
@Slf4j
public final class CassandraHelper {

    private CassandraHelper() {
    }

    /**
     * 校验entityClass必须符合框架的规范
     *
     * @param entityClass
     */
    static void checkEntityClass(Class entityClass) {
        if (entityClass == null) {
            throw new RuntimeException("can not get the entity's Generic Type");
        }

        String entityClassName = entityClass.getName();
        CassandraTable tableAnnotation = (CassandraTable) entityClass.getAnnotation(CassandraTable.class);
        if (tableAnnotation == null) {
            throw new RuntimeException("entity[" + entityClassName + "] must have CassandraTable annotation");
        }

        Field[] fields = entityClass.getDeclaredFields();
        if (fields == null || fields.length == 0) {
            throw new RuntimeException("entity[" + entityClassName + "] must have least one Field");
        }
    }

    static String getKeyspace(Class entityClass) {
        CassandraTable documentAnn = (CassandraTable) entityClass.getAnnotation(CassandraTable.class);
        return documentAnn.keyspace();
    }

    static String getTableName(Class entityClass) {
        CassandraTable documentAnn = (CassandraTable) entityClass.getAnnotation(CassandraTable.class);
        return documentAnn.tableName();
    }

    static <T> List<T> getEntityList(ResultSet resultSet, Class<T> entityClass, Map<String, Class> fieldNameAndFieldClassMap) {
        List<T> entityList = Lists.newArrayList();
        Iterator<Row> iterator = resultSet.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            T entity = getEntity(row, entityClass, fieldNameAndFieldClassMap);
            if (entity != null) {
                entityList.add(entity);
            }
        }
        return entityList;
    }

    static <T> T getEntity(Row row, Class<T> entityClass, Map<String, Class> fieldNameAndFieldClassMap) {
        if (row == null) {
            return null;
        }
        Map<String, Object> map = Maps.newHashMap();
        try {
            for (Map.Entry<String, Class> entry : fieldNameAndFieldClassMap.entrySet()) {
                String fieldName = entry.getKey();
                if (!row.getColumnDefinitions().contains(fieldName)) {
                    continue;
                }
                Object value = row.get(fieldName, entry.getValue());
                map.put(entry.getKey(), value);
            }
            return BeanUtils.mapToBean(map, entityClass);
        } catch (RuntimeException e) {
            log.error("CassandraHelper.getEntity error", e);
        }
        return null;
    }
}
