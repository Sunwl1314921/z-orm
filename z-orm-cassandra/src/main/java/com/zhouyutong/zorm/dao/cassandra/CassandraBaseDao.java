package com.zhouyutong.zorm.dao.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zhouyutong.zorm.dao.AbstractBaseDao;
import com.zhouyutong.zorm.dao.DaoHelper;
import com.zhouyutong.zorm.enums.DialectEnum;
import com.zhouyutong.zorm.query.Criteria;
import com.zhouyutong.zorm.query.Pageable;
import com.zhouyutong.zorm.query.Query;
import com.zhouyutong.zorm.query.Update;
import com.zhouyutong.zorm.utils.ExceptionTranslator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于cassandra-driver-core的Dao实现
 *
 * @author zhoutao
 * @date 2018/5/11
 */
@Slf4j
public abstract class CassandraBaseDao<T> extends AbstractBaseDao<T> implements ApplicationContextAware {
    private Class<T> entityClass;
    private CassandraSettings cassandraSettings;
    private String keyspace;
    private String tableName;
    private Map<String, Class> fieldNameAndFieldClassMap = Maps.newHashMap();
    private ApplicationContext applicationContext;

    @Override
    public Class<T> getGenericClass() {
        return this.entityClass;
    }

    @Override
    public boolean exists(Serializable id) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public boolean exists(Criteria criteria) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public long countByCriteria(Criteria criteria) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public long countAll() {
        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        String countAllCql = "select count(*) from " + keyspace + "." + tableName;
        try {
            ResultSet resultSet = session.execute(countAllCql);
            long count = resultSet == null ? 0L : resultSet.one().getLong(1);
            return count;
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    protected long countBySql(String sql, LinkedHashMap<String, Object> param) {
        DaoHelper.checkArgument(sql);
        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);

        List<Object> valueList = MapUtils.isEmpty(param) ? null : Lists.newArrayList(param.values());
        try {
            if (log.isDebugEnabled()) {
                log.debug("=========countBySql request:" + DaoHelper.formatSql(sql.toString(), valueList));
            }
            ResultSet resultSet = null;
            if (CollectionUtils.isEmpty(valueList)) {
                resultSet = session.execute(sql);
            } else {
                resultSet = session.execute(sql, valueList.toArray());
            }
            if (log.isDebugEnabled()) {
                log.debug("=========countBySql response:" + resultSet);
            }
            long count = resultSet == null ? 0L : resultSet.one().getLong(1);
            return count;
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public T findOneById(Serializable id) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public T findOneByQuery(Query query) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    protected T findOneBySql(String sql, LinkedHashMap<String, Object> param) {
        DaoHelper.checkArgument(sql);

        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        List<Object> valueList = MapUtils.isEmpty(param) ? null : Lists.newArrayList(param.values());
        try {
            if (log.isDebugEnabled()) {
                log.debug("=========findOneBySql request:" + DaoHelper.formatSql(sql.toString(), valueList));
            }
            ResultSet resultSet;
            if (CollectionUtils.isEmpty(valueList)) {
                resultSet = session.execute(sql);
            } else {
                resultSet = session.execute(sql, valueList.toArray());
            }
            if (log.isDebugEnabled()) {
                log.debug("=========findOneBySql response:" + resultSet);
            }
            return CassandraHelper.getEntity(resultSet.one(), entityClass, fieldNameAndFieldClassMap);
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public List<T> findListByIds(List<Serializable> ids) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public List<T> findListByQuery(Query query) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public List<T> findListByQuery(Query query, Pageable pageable) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    protected List<T> findListBySql(String sql, LinkedHashMap<String, Object> param) {
        DaoHelper.checkArgument(sql);

        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        List<Object> valueList = MapUtils.isEmpty(param) ? null : Lists.newArrayList(param.values());
        try {
            if (log.isDebugEnabled()) {
                log.debug("=========findListBySql request:" + DaoHelper.formatSql(sql.toString(), valueList));
            }
            ResultSet resultSet;
            if (CollectionUtils.isEmpty(valueList)) {
                resultSet = session.execute(sql);
            } else {
                resultSet = session.execute(sql, valueList.toArray());
            }
            if (log.isDebugEnabled()) {
                log.debug("=========findListBySql response:" + resultSet);
            }
            return CassandraHelper.getEntityList(resultSet, entityClass, fieldNameAndFieldClassMap);
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public int insert(T entity) {
        DaoHelper.checkArgumentEntity(entity);
        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        Insert insert = QueryBuilder.insertInto(keyspace, tableName);
        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            String propertyName = field.getName();
            Object propertyValue = DaoHelper.getColumnValue(propertyName, entity);
            insert.value(propertyName, propertyValue);
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("=========insert request:" + insert.toString());
            }
            ResultSet resultSet = session.execute(insert);
            if (log.isDebugEnabled()) {
                log.debug("=========insert response:" + resultSet.toString());
            }
            return 1;
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public int insert(List<T> entityList) {
        DaoHelper.checkArgumentBatchInsert(entityList);
        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);

        BatchStatement batchStatement = new BatchStatement();
        for (T entity : entityList) {
            Insert insert = QueryBuilder.insertInto(keyspace, tableName);
            Field[] fields = entityClass.getDeclaredFields();
            for (Field field : fields) {
                String propertyName = field.getName();
                Object propertyValue = DaoHelper.getColumnValue(propertyName, entity);
                insert.value(propertyName, propertyValue);
            }
            batchStatement.add(insert);
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("=========batch insert request:" + batchStatement.toString());
            }
            ResultSet resultSet = session.execute(batchStatement);
            batchStatement.clear();
            if (log.isDebugEnabled()) {
                log.debug("=========batch insert response:" + resultSet.toString());
            }
            return entityList.size();
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public int update(T entity) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public int update(T entity, List<String> propetyList) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public int updateById(Serializable id, Update update) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public int updateByIds(List<Serializable> ids, Update update) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public int updateByCriteria(Criteria criteria, Update update) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    protected int updateBySql(String sql, LinkedHashMap<String, Object> param) {
        DaoHelper.checkArgument(sql);

        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        List<Object> valueList = MapUtils.isEmpty(param) ? null : Lists.newArrayList(param.values());
        try {
            if (log.isDebugEnabled()) {
                log.debug("=========updateBySql request:" + DaoHelper.formatSql(sql.toString(), valueList));
            }
            ResultSet resultSet;
            if (CollectionUtils.isEmpty(valueList)) {
                resultSet = session.execute(sql);
            } else {
                resultSet = session.execute(sql, valueList.toArray());
            }
            if (log.isDebugEnabled()) {
                log.debug("=========updateBySql response:" + resultSet);
            }
            return 1;
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @Override
    public int deleteById(Serializable id) {
        throw new RuntimeException("CassandraBaseDao do not support The Method");
    }

    @Override
    public int deleteBySql(String sql, LinkedHashMap<String, Object> param) {
        DaoHelper.checkArgument(sql);

        Session session = CassandraClientFactory.INSTANCE.getClient(cassandraSettings);
        List<Object> valueList = MapUtils.isEmpty(param) ? null : Lists.newArrayList(param.values());
        try {
            if (log.isDebugEnabled()) {
                log.debug("=========deleteBySql request:" + DaoHelper.formatSql(sql.toString(), valueList));
            }
            ResultSet resultSet;
            if (CollectionUtils.isEmpty(valueList)) {
                resultSet = session.execute(sql);
            } else {
                resultSet = session.execute(sql, valueList.toArray());
            }
            if (log.isDebugEnabled()) {
                log.debug("=========deleteBySql response:" + resultSet);
            }
            return 1;
        } catch (RuntimeException e) {
            throw ExceptionTranslator.translate(e, DialectEnum.CASSANDRA);
        }
    }

    @PostConstruct
    protected void afterPropertiesSet() {
        Class daoClass = this.getClass();
        //得到泛型entityClass
        ParameterizedType type = (ParameterizedType) daoClass.getGenericSuperclass();
        Type[] p = type.getActualTypeArguments();
        this.entityClass = (Class<T>) p[0];
        CassandraHelper.checkEntityClass(this.entityClass);
        this.keyspace = CassandraHelper.getKeyspace(entityClass);
        this.tableName = CassandraHelper.getTableName(entityClass);

        //得到settings
        String settingsName = DaoHelper.getSettingsName(daoClass);
        this.cassandraSettings = (CassandraSettings) this.applicationContext.getBean(settingsName);
        if (this.cassandraSettings == null) {
            throw new RuntimeException("注解Dao的属性settingBeanName[" + settingsName + "]必须对应一个有效的CassandraSettings bean");
        }

        //得到所有字段名和对应类型
        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            fieldNameAndFieldClassMap.put(field.getName(), field.getType());
        }
        CassandraClientFactory.INSTANCE.setClient(this.cassandraSettings);
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
