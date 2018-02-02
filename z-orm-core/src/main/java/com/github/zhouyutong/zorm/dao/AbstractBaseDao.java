package com.github.zhouyutong.zorm.dao;

import com.github.zhouyutong.zorm.exception.DaoException;
import com.github.zhouyutong.zorm.query.*;

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

    public abstract int insert(T entity) throws DaoException;

    //更新实体所有属性
    public abstract int update(T entity) throws DaoException;

    //更新实体中指定的属性
    public abstract int update(T entity, List<String> propetyList) throws DaoException;

    public abstract int updateById(Serializable id, Update update) throws DaoException;

    public abstract int updateByIds(List<Serializable> ids, Update update) throws DaoException;

    public abstract int updateByCriteria(Criteria criteria, Update update) throws DaoException;

    protected abstract int updateBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException;

    public abstract int deleteById(Serializable id) throws DaoException;

    public abstract boolean exists(Serializable id) throws DaoException;

    public abstract boolean exists(Criteria criteria) throws DaoException;

    public abstract long countByCriteria(Criteria criteria) throws DaoException;

    public abstract long countAll() throws DaoException;

    protected abstract long countBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException;

    public abstract T findOne(List<String> fields, Criteria criteria) throws DaoException;

    public abstract T findOne(Criteria criteria) throws DaoException;

    public abstract T findOneById(Serializable id) throws DaoException;

    public abstract T findOneByQuery(Query query) throws DaoException;

    protected abstract T findOneBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException;

    public abstract List<T> findListByIds(List<Serializable> ids) throws DaoException;

    public abstract List<T> findListByQuery(Query query) throws DaoException;

    public abstract List<T> findListByQuery(Query query, Pageable pageable) throws DaoException;

    protected abstract List<T> findListBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException;

    public abstract List<T> findList(List<String> fields, Criteria criteria) throws DaoException;

    public abstract List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys) throws DaoException;

    public abstract List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys, Pageable pageable) throws DaoException;

    public abstract List<T> findList(Criteria criteria) throws DaoException;

    public abstract List<T> findList(Criteria criteria, List<OrderBy> orderBys) throws DaoException;

    public abstract List<T> findList(Criteria criteria, List<OrderBy> orderBys, Pageable pageable) throws DaoException;

    public abstract List<T> findAllList() throws DaoException;

    public abstract List<T> findAllList(List<String> fields) throws DaoException;

    public abstract List<T> findAllList(List<String> fields, List<OrderBy> orderBys) throws DaoException;

    public abstract List<T> findAllList(List<String> fields, List<OrderBy> orderBys, Pageable pageable) throws DaoException;

    public abstract List<T> findAllList(List<OrderBy> orderBys, Pageable pageable) throws DaoException;

}
