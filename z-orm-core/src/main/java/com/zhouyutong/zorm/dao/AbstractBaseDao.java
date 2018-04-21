package com.zhouyutong.zorm.dao;

import com.zhouyutong.zorm.query.*;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 基础DAO接口 封装常用的CRUD操作,与具体orm框架无关
 *
 * @author zhouyutong
 * @since 2015/11/24
 */
public abstract class AbstractBaseDao<T> {

    public abstract Class<T> getGenericClass();

    public abstract int insert(T entity);

    //更新实体所有属性
    public abstract int update(T entity);

    //更新实体中指定的属性
    public abstract int update(T entity, List<String> propetyList);

    public abstract int updateById(Serializable id, Update update);

    public abstract int updateByIds(List<Serializable> ids, Update update);

    public abstract int updateByCriteria(Criteria criteria, Update update);

    protected abstract int updateBySql(String sql, LinkedHashMap<String, Object> param);

    public abstract int deleteById(Serializable id);

    public abstract boolean exists(Serializable id);

    public abstract boolean exists(Criteria criteria);

    public abstract long countByCriteria(Criteria criteria);

    public abstract long countAll();

    protected abstract long countBySql(String sql, LinkedHashMap<String, Object> param);

    public abstract T findOneById(Serializable id);

    public abstract T findOneByQuery(Query query);

    protected abstract T findOneBySql(String sql, LinkedHashMap<String, Object> param);

    public abstract List<T> findListByIds(List<Serializable> ids);

    public abstract List<T> findListByQuery(Query query);

    public abstract List<T> findListByQuery(Query query, Pageable pageable);

    protected abstract List<T> findListBySql(String sql, LinkedHashMap<String, Object> param);

    public T findOne(List<String> fields, Criteria criteria) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findOneByQuery(query);
    }

    public T findOne(Criteria criteria) {
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        return this.findOneByQuery(query);
    }

    public List<T> findList(List<String> fields, Criteria criteria) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findListByQuery(query);
    }

    public List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    public List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys, Pageable pageable) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    public List<T> findList(Criteria criteria) {
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        return this.findListByQuery(query);
    }

    public List<T> findList(Criteria criteria, List<OrderBy> orderBys) {
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query(criteria);
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    public List<T> findList(Criteria criteria, List<OrderBy> orderBys, Pageable pageable) {
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query(criteria);
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    public List<T> findAllList() {
        Query query = Query.query();
        return this.findListByQuery(query);
    }

    public List<T> findAllList(List<String> fields) {
        DaoHelper.checkArgumentFields(fields);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findListByQuery(query);
    }

    public List<T> findAllList(List<String> fields, List<OrderBy> orderBys) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    public List<T> findAllList(List<String> fields, List<OrderBy> orderBys, Pageable pageable) {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    public List<T> findAllList(List<OrderBy> orderBys, Pageable pageable) {
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query();
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

}
