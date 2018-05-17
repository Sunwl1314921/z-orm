package com.zhouyutong.zorm.dao.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zhouyutong.zorm.annotation.PK;
import com.zhouyutong.zorm.constant.MixedConstant;
import com.zhouyutong.zorm.constant.SymbolConstant;
import com.zhouyutong.zorm.dao.DaoHelper;
import com.zhouyutong.zorm.dao.jdbc.annotation.Column;
import com.zhouyutong.zorm.dao.jdbc.annotation.Table;
import com.zhouyutong.zorm.entity.IdEntity;
import com.zhouyutong.zorm.enums.DialectEnum;
import com.zhouyutong.zorm.query.*;
import com.zhouyutong.zorm.utils.BeanUtils;
import com.zhouyutong.zorm.utils.ExceptionTranslator;
import com.zhouyutong.zorm.utils.StrUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.support.JdbcUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * jdbcBaseDao内部帮助类
 * 用于拼装sql
 *
 * @author zhouyutong
 * @since 2015/11/24
 */
public final class JdbcHelper {
    private JdbcHelper() {
    }

    static String SELECT_COUNT() {
        return "SELECT COUNT(*) ";
    }

    static String SELECT(Query query, EntityMapper<?> entityMapper) {
        Map<String, String> propertyToColumnMapper = entityMapper.getPropertyToColumnMapper();
        List<String> fields = query.getFields();
        List<GroupBy> groupBys = query.getGroupBys();

        StringBuilder sb = new StringBuilder("SELECT ");

        if (CollectionUtils.isNotEmpty(groupBys)) {
            for (GroupBy groupBy : groupBys) {
                String groupByKey = groupBy.getKey();
                sb.append(propertyToColumnMapper.get(groupByKey)).append(SymbolConstant.COMMA);

                String groupCountAlias = groupBy.getGroupCountAlias();
                if (StringUtils.isNotBlank(groupCountAlias)) {   //需要每组数量groupCountAlias是entity中定义的非持久化字段
                    sb.append("COUNT(*) AS " + propertyToColumnMapper.get(groupCountAlias)).append(SymbolConstant.COMMA);
                }
            }
        } else {
            if (CollectionUtils.isNotEmpty(fields)) {
                for (String field : fields) {
                    String column = propertyToColumnMapper.get(field);
                    sb.append(column).append(SymbolConstant.COMMA);
                }
            } else {
                for (String column : propertyToColumnMapper.values()) {
                    sb.append(column).append(SymbolConstant.COMMA);
                }
            }
        }

        sb.deleteCharAt(sb.length() - MixedConstant.INT_1);//去掉最后一个,
        sb.append(SymbolConstant.BLANK);
        return sb.toString();
    }

    static String DELETE(Class<?> entityClass) {
        return "DELETE " + FROM(entityClass) + " WHERE id = ?";
    }

    static String FROM(Class<?> entityClass) {
        return "FROM " + getTableName(entityClass) + SymbolConstant.BLANK;
    }

    static String GROUP_BY(List<GroupBy> groupByList, EntityMapper entityMapper) {
        if (CollectionUtils.isEmpty(groupByList)) {
            return SymbolConstant.EMPTY;
        }

        StringBuilder sb = new StringBuilder("GROUP BY ");
        for (GroupBy groupBy : groupByList) {
            sb.append(entityMapper.getPropertyToColumnMapper().get(groupBy.getKey())).append(SymbolConstant.COMMA);
        }
        sb.deleteCharAt(sb.length() - MixedConstant.INT_1);//去掉最后一个,
        sb.append(SymbolConstant.BLANK);
        return sb.toString();
    }

    static String ORDER_BY(List<OrderBy> orderByList, EntityMapper entityMapper) {
        if (CollectionUtils.isEmpty(orderByList)) {
            return SymbolConstant.EMPTY;
        }

        StringBuilder sb = new StringBuilder("ORDER BY ");
        for (OrderBy orderBy : orderByList) {
            sb.append(entityMapper.getPropertyToColumnMapper().get(orderBy.getKey())).append(SymbolConstant.BLANK).append(orderBy.getDirection()).append(SymbolConstant.COMMA);
        }
        sb.deleteCharAt(sb.length() - MixedConstant.INT_1);//去掉最后一个,
        sb.append(SymbolConstant.BLANK);
        return sb.toString();
    }

    static String LIMIT(int offset, int limit, DialectEnum dialectEnum, final StringBuilder sql) {
        if (offset < MixedConstant.INT_0 || limit <= MixedConstant.INT_0) {
            return SymbolConstant.EMPTY;
        }
        StringBuilder newSql = new StringBuilder();
        if (DialectEnum.MYSQL.equals(dialectEnum)) {
            newSql.append(sql.toString());
            newSql.append("LIMIT ");
            newSql.append(offset).append(SymbolConstant.COMMA).append(limit);
        } else if (DialectEnum.ORACLE.equals(dialectEnum)) {
            newSql.append("SELECT * FROM (")
                    .append("SELECT ROWNUM AS RN,table_alias.* FROM (")
                    .append(sql.toString())
                    .append(") table_alias ")
                    .append("WHERE ROWNUM <= ")
                    .append(offset + limit)
                    .append(") ")
                    .append("WHERE RN > ").append(offset);

        }
        sql.setLength(MixedConstant.INT_0);
        return newSql.toString();
    }

    static String UPDATE(Class<?> entityClass) {
        return "UPDATE " + getTableName(entityClass) + SymbolConstant.BLANK;
    }

    static String SET(Update update, List<Object> valueList, EntityMapper<?> entityMapper) {
        Map<String, String> propertyToColumnMapper = entityMapper.getPropertyToColumnMapper();
        Set<String> notNeedTransientPropertySet = entityMapper.getNotNeedTransientPropertySet();

        StringBuilder sb = new StringBuilder("SET ");
        Set<String> keySet = update.getSetMap().keySet();
        for (String key : keySet) {
            if (notNeedTransientPropertySet.contains(key)) {
                continue;
            }
            sb.append(propertyToColumnMapper.get(key)).append("=?,");
            valueList.add(update.get(key));
        }
        sb.deleteCharAt(sb.length() - MixedConstant.INT_1);//去掉最后一个,
        sb.append(SymbolConstant.BLANK);
        return sb.toString();
    }

    static <T> T map2Entity(Map<String, Object> map, EntityMapper<T> entityMapper, Class<T> entityClass) {
        Map<String, String> columnToPropertyMapper = entityMapper.getColumnToPropertyMapper();

        try {
            HashMap<String, Object> propertyMap = Maps.newHashMap();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String k = entry.getKey();
                Object v = entry.getValue();
                //如果select出来的字段有而entity没有对应的属性，需要忽略该字段
                String propertyName = columnToPropertyMapper.get(k.toLowerCase());
                if (propertyName == null) {
                    continue;
                }
                propertyMap.put(propertyName, v);
            }
            return BeanUtils.mapToBean(propertyMap, entityClass);
        } catch (Exception e) {
            throw new RuntimeException("map2Entity无法将数据库中行记录[" + map + "]转换成Entity对象[" + entityClass.getSimpleName() + "]", e);
        }
    }

    /**
     * 根据criteria拼装sql where
     *
     * @param criteria     - 条件对象
     * @param valueList    - 值列表
     * @param entityMapper - entityMapper
     * @return where sql
     */
    static String WHERE(Criteria criteria, List<Object> valueList, EntityMapper<?> entityMapper) {
        if (criteria == null) {
            return SymbolConstant.EMPTY;
        }

        Map<String, String> propertyToColumnMapper = entityMapper.getPropertyToColumnMapper();

        StringBuilder whereSB = new StringBuilder();
        //criteria一定不为null，且criteria.getCriteriaChain()一定不为empty
        List<Criteria> criterias = criteria.getCriteriaChain();
        for (Criteria c : criterias) {
            if (CriteriaOperators.isNoValueOperator(c.getOperator())) {
                whereSB.append(" AND ").append(propertyToColumnMapper.get(c.getKey())).append(SymbolConstant.BLANK).append(c.getOperator());
            } else if (CriteriaOperators.isSingleValueOperator(c.getOperator())) {
                whereSB.append(" AND ").append(propertyToColumnMapper.get(c.getKey())).append(SymbolConstant.BLANK).append(c.getOperator()).append(" ?");
                valueList.add(c.getValue());
            } else if (CriteriaOperators.isMultiValueOperator(c.getOperator())) {
                whereSB.append(" AND ").append(propertyToColumnMapper.get(c.getKey())).append(SymbolConstant.BLANK).append(c.getOperator()).append(" (");
                Collection collection = (Collection) c.getValue();
                for (Object item : collection) {
                    whereSB.append(SymbolConstant.QUESTION).append(SymbolConstant.COMMA);
                    valueList.add(item);
                }
                whereSB.deleteCharAt(whereSB.length() - MixedConstant.INT_1);
                whereSB.append(")");
            }
        }
        String whereSql = "WHERE " + whereSB.delete(MixedConstant.INT_0, MixedConstant.INT_5).toString() + SymbolConstant.BLANK;//去掉第一个 and
        return whereSql;
    }

    /**
     * 生成oracle id
     *
     * @param sequence   - sequence名字
     * @param connection - 链接
     * @return - id
     */
    private static Long genOracleId(String sequence, Connection connection) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement("SELECT " + sequence + ".NEXTVAL AS ID FROM DUAL");
            rs = pstmt.executeQuery();
            rs.next();
            return rs.getLong(MixedConstant.INT_1);
        } catch (SQLException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.ORACLE);
        } finally {
            JdbcUtils.closeResultSet(rs);
            JdbcUtils.closeStatement(pstmt);
        }
    }

    static String INSERT(IdEntity idEntity, List<Object> valueList, EntityMapper<?> entityMapper, Class<?> entityClass, DialectEnum dialectEnum, Connection connection) {
        Map<String, String> propertyToColumnMapper = entityMapper.getPropertyToColumnMapper();
        Set<String> notNeedTransientPropertySet = entityMapper.getNotNeedTransientPropertySet();

        StringBuilder sb = new StringBuilder(" INSERT INTO ");

        //组装表名
        sb.append(getTableName(entityClass)).append(" (");

        //组装字段
        List<String> pList = Lists.newArrayList();

        //IdEntity字段
        final Field pkField = DaoHelper.getPkField(idEntity);
        final Object pkValue = DaoHelper.getColumnValue(pkField, idEntity);
        if (DaoHelper.hasSetPkValue(pkValue)) {
            pList.add(SymbolConstant.QUESTION);
            sb.append(propertyToColumnMapper.get(entityMapper.getPkFieldName())).append(SymbolConstant.COMMA);
            valueList.add(pkValue);
        } else {
            if (DialectEnum.ORACLE.equals(dialectEnum)) {
                sb.append(propertyToColumnMapper.get(entityMapper.getPkFieldName())).append(SymbolConstant.COMMA);
                if (StringUtils.isNotBlank(getSequenceName(entityClass))) {
                    pList.add(SymbolConstant.QUESTION);
                    Long oracleId = genOracleId(getSequenceName(entityClass), connection);
                    valueList.add(oracleId);
                    DaoHelper.setColumnValue(pkField, idEntity, oracleId);
                } else {
                    throw new RuntimeException("连接ORACLE,实体Table注解必须设置sequence");
                }
            }
        }

        //本类字段
        for (Map.Entry<String, String> entry : propertyToColumnMapper.entrySet()) {
            String fieldName = entry.getKey();
            String columnName = entry.getValue();
            //pk前面已经处理了
            if (fieldName.equals(entityMapper.getPkFieldName())) {
                continue;
            }
            //过滤掉不需要持久化的
            if (notNeedTransientPropertySet.contains(fieldName)) {
                continue;
            }

            sb.append(columnName).append(SymbolConstant.COMMA);
            pList.add(SymbolConstant.QUESTION);
            valueList.add(DaoHelper.getColumnValue(fieldName, idEntity));
        }
        sb.deleteCharAt(sb.length() - MixedConstant.INT_1).append(")");

        //组装值
        sb.append(" VALUES (");
        for (String p : pList) {
            sb.append(p).append(SymbolConstant.COMMA);
        }
        sb.deleteCharAt(sb.length() - MixedConstant.INT_1).append(") ");
        return sb.toString();
    }

    /**
     * 根据entity的class获取对应的表名
     *
     * @param entityClass - entityClass
     * @return - 注解标注的表名
     */
    static String getTableName(Class<?> entityClass) {
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        return tableAnnotation.value();
    }

    /**
     * 根据entity的class获取对应的sequence
     *
     * @param entityClass - entityClass
     * @return - 注解标注的sequence
     */
    static String getSequenceName(Class<?> entityClass) {
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        return tableAnnotation.sequence();
    }

    /**
     * 校验entityClass必须符合框架的规范
     *
     * @param entityClass
     */
    public static void checkEntityClass(Class entityClass) {
        if (entityClass == null) {
            throw new RuntimeException("can not get the entity's Generic Type");
        }

        String entityClassName = entityClass.getName();
        if (!IdEntity.class.isAssignableFrom(entityClass)) {
            throw new RuntimeException("entity[" + entityClassName + "] must implements IdEntity");
        }

        Table tableAnnotation = (Table) entityClass.getAnnotation(Table.class);
        if (tableAnnotation == null) {
            throw new RuntimeException("entity[" + entityClassName + "] must have Table annotation");
        }

        Field[] fields = entityClass.getDeclaredFields();
        if (fields == null || fields.length == 0) {
            throw new RuntimeException("entity[" + entityClassName + "] must have least one Field");
        }

        int pkAnnotationCount = 0;
        String pkFieldTypeName = "";
        List<String> supportPKFieldType = Lists.newArrayList("java.lang.Integer", "java.lang.Long", "java.lang.String");
        for (Field field : fields) {
            if (DaoHelper.isFinalOrStatic(field)) {
                continue;
            }
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation == null) {
                throw new RuntimeException("entity[" + entityClassName + "]的字段[" + field.getName() + "]必须有Column注解");
            }

            PK pkAnnotation = field.getAnnotation(PK.class);
            if (pkAnnotation != null) {
                pkAnnotationCount++;
                pkFieldTypeName = field.getType().getName();
            }
        }
        if (pkAnnotationCount != 1) {
            throw new RuntimeException("entity[" + entityClassName + "] 有且只能有一个PK注解的字段");
        }
        if (!supportPKFieldType.contains(pkFieldTypeName)) {
            throw new RuntimeException("entity[" + entityClassName + "]的pk字段类型只能是Long,Integer,String其中之一");
        }
    }

    /**
     * 根据field得到对应列名
     *
     * @param field - 字段对象
     * @return - 返回filed标注的列名注解
     */
    public static String getColumnName(Field field) {
        String propertyName = field.getName();
        Column columnAnnotation = field.getAnnotation(Column.class);
        if (StringUtils.isBlank(columnAnnotation.value())) {//column注解没有值,采用驼峰法取字段名
            return StrUtils.underscoreName(propertyName);
        } else {//使用column注解定义的字段名
            return columnAnnotation.value().toLowerCase();
        }
    }
}
