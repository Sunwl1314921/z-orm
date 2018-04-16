package com.zhouyutong.zorm.dao.jdbc;

import com.zhouyutong.zorm.annotation.PK;
import com.zhouyutong.zorm.dao.DaoHelper;
import com.zhouyutong.zorm.dao.jdbc.annotation.Column;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

/**
 * 实体和数据库的映射
 *
 * @Author zhouyutong
 * @Date 2017/6/8
 */
@Getter
public final class EntityMapper<T> {
    private String entityName;
    private String pkFieldName;
    //属性名到字段名映射
    private Map<String, String> propertyToColumnMapper = Maps.newLinkedHashMap();
    //字段名到属性名映射
    private Map<String, String> columnToPropertyMapper = Maps.newLinkedHashMap();
    //不需要持久化的字段
    private Set<String> notNeedTransientPropertySet = Sets.newHashSet();

    public EntityMapper(Class<T> entityClass) {
        this.entityName = entityClass.getCanonicalName();

        try {
            //本类字段
            Field[] fields = entityClass.getDeclaredFields();
            for (Field field : fields) {
                if (DaoHelper.isFinalOrStatic(field)) {
                    continue;
                }
                String propertyName = field.getName();
                //istransient=true的加入到忽略持久化列表
                Column columnAnnotation = field.getAnnotation(Column.class);
                if (!columnAnnotation.isTransient()) {
                    notNeedTransientPropertySet.add(propertyName);
                }

                if (field.getAnnotation(PK.class) != null) {
                    pkFieldName = propertyName;
                }

                String columnName = JdbcHelper.getColumnName(field);
                propertyToColumnMapper.put(propertyName, columnName);
                columnToPropertyMapper.put(columnName, propertyName);
            }
        } catch (Exception e) {
            throw new RuntimeException("无法创建Entity[" + getEntityName() + "]对应的EntityMapper", e);
        }
    }
}
