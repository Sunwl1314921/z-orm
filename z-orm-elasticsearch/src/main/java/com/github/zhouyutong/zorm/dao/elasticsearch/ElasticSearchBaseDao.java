package com.github.zhouyutong.zorm.dao.elasticsearch;

import com.github.zhouyutong.zorm.annotation.PK;
import com.github.zhouyutong.zorm.constant.DBConstant;
import com.github.zhouyutong.zorm.constant.MixedConstant;
import com.github.zhouyutong.zorm.dao.DaoHelper;
import com.github.zhouyutong.zorm.dao.IBaseDao;
import com.github.zhouyutong.zorm.entity.IdEntity;
import com.github.zhouyutong.zorm.exception.DaoException;
import com.github.zhouyutong.zorm.exception.DaoExceptionTranslator;
import com.github.zhouyutong.zorm.exception.DaoMethodParameterException;
import com.github.zhouyutong.zorm.query.*;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 基于ElasticSearch 5.3 TransportClient的Dao实现
 *
 * @author zhouyutong
 * @Date 2017/4/20
 */
@Slf4j
public abstract class ElasticSearchBaseDao<T> implements ApplicationContextAware, IBaseDao<T> {

    private ElasticSearchSettings elasticSearchSettings;
    private String index;
    private String type;
    private String pkFieldName;
    private Class<T> entityClass;
    private boolean hasEsVersionFiled;  //含有es的version字段可使用ES的带版本更新
    private List<String> notNeedTransientPropertyList = Lists.newArrayList();   //不需要持久化的字段
    private ApplicationContext applicationContext;

    @Override
    public Class<T> getGenericClass() {
        return this.entityClass;
    }

    @Override
    public boolean exists(Serializable id) throws DaoException {
        DaoHelper.checkArgumentId(id);

        return this.exists(Criteria.where(pkFieldName, id));
    }

    @Override
    public boolean exists(Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);

        return null != this.findOne(Arrays.asList(pkFieldName), criteria);
    }

    @Override
    public long countByCriteria(Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            QueryBuilder queryBuilder = ElasticSearchHelper.criteria2QueryBuilder(criteria);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(false)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_0);    //es自动转换为count模式
            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }

            if (log.isDebugEnabled()) {
                log.debug("countByCriteria searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            return searchResponse.getHits().getTotalHits();
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public long countAll() throws DaoException {
        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(false)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_0);    //es自动转换为count模式

            if (log.isDebugEnabled()) {
                log.debug("countAll searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            return searchResponse.getHits().getTotalHits();
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public long countBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);
        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(sql);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(false)
                    .setQuery(queryBuilder)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_0);    //es自动转换为count模式
            if (log.isDebugEnabled()) {
                log.debug("countBySql searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            return searchResponse.getHits().getTotalHits();
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOneById(Serializable id) throws DaoException {
        DaoHelper.checkArgumentId(id);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            GetResponse response = client.prepareGet()
                    .setIndex(index)
                    .setType(type)
                    .setId(ElasticSearchHelper.getIdSerializable(id))
                    .setOperationThreaded(false)
                    .get();
            if (!response.isExists()) {
                return null;
            }
            String source = ElasticSearchHelper.setEsVersion(response, hasEsVersionFiled);
            return FastJson.jsonStr2Object(source, entityClass);
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOneByQuery(Query query) throws DaoException {
        DaoHelper.checkArgumentQuery(query);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            QueryBuilder queryBuilder = ElasticSearchHelper.criteria2QueryBuilder(query.getCriteria());
            String[] includes = ElasticSearchHelper.includeFileds(query.getFields());
            String[] excludes = MixedConstant.EMPTY_STRING_ARRAY;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(includes, excludes)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_1);

            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            if (log.isDebugEnabled()) {
                log.debug("findOneByQuery searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            return ElasticSearchHelper.getEntity(searchResponse, entityClass, hasEsVersionFiled);
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOneBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(sql);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setQuery(queryBuilder)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_1);

            if (log.isDebugEnabled()) {
                log.debug("findOneBySql searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            return ElasticSearchHelper.getEntity(searchResponse, entityClass, hasEsVersionFiled);
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public List<T> findListByIds(List<Serializable> ids) throws DaoException {
        DaoHelper.checkArgumentIds(ids);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                    .add(index, type, (Iterable) ids)
                    .get();
            List<T> entityList = Lists.newArrayList();
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String source = ElasticSearchHelper.setEsVersion(response, hasEsVersionFiled);
                    entityList.add(FastJson.jsonStr2Object(source, entityClass));
                }
            }
            return entityList;
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public List<T> findListByQuery(Query query) throws DaoException {
        DaoHelper.checkArgumentQuery(query);

        try {

            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            String[] includes = ElasticSearchHelper.includeFileds(query.getFields());
            String[] excludes = MixedConstant.EMPTY_STRING_ARRAY;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(includes, excludes);

            QueryBuilder queryBuilder = ElasticSearchHelper.criteria2QueryBuilder(query.getCriteria());
            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            int from = query.getOffset() < MixedConstant.INT_0 ? MixedConstant.INT_0 : query.getOffset();
            int size = query.getLimit() < MixedConstant.INT_1 ? Integer.MAX_VALUE : query.getLimit();

            //支持简单聚合 TODO：复杂聚合需要使用bySql
            if (CollectionUtils.isNotEmpty(query.getGroupBys())) {
                searchRequestBuilder.setSize(MixedConstant.INT_0);
                searchRequestBuilder.setFetchSource(false);
                TermsAggregationBuilder termsAggregationBuilder = ElasticSearchHelper.groupBy2AggregationBuilder(query.getGroupBys());
                termsAggregationBuilder.size(size);
                searchRequestBuilder.addAggregation(termsAggregationBuilder);
                if (log.isDebugEnabled()) {
                    log.debug("findListByQuery searchRequestBuilder:" + searchRequestBuilder.toString());
                }
                SearchResponse searchResponse = searchRequestBuilder.get();
                return ElasticSearchHelper.getAggregationEntityList(searchResponse, entityClass, query.getGroupBys());
            } else {
                if (CollectionUtils.isNotEmpty(query.getOrderBys())) {
                    for (OrderBy orderBy : query.getOrderBys()) {
                        String field = orderBy.getKey();
                        String direction = orderBy.getDirection();
                        SortOrder order = OrderBy.Direction.ASC.getDirection().equals(direction) ? SortOrder.ASC : SortOrder.DESC;
                        searchRequestBuilder.addSort(field, order);
                    }
                }

                queryOverloadProtect(searchRequestBuilder, from, size, queryBuilder);
                return nonScrollQuery(searchRequestBuilder);
            }
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public List<T> findListByQuery(Query query, Pageable pageable) throws DaoException {
        DaoHelper.checkArgumentQuery(query);
        DaoHelper.checkArgumentPageable(pageable);

        int limit = pageable.getPageSize();
        int offset = (pageable.getPageNumber() - 1) * limit;
        query.offset(offset).limit(limit);
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findListBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type);

            QueryBuilder queryBuilder = null;
            if (StringUtils.isNotBlank(sql)) {
                queryBuilder = QueryBuilders.wrapperQuery(sql);
                searchRequestBuilder.setQuery(queryBuilder);
            }

            int from = MapUtils.getIntValue(param, "form", MixedConstant.INT_0);
            int size = MapUtils.getIntValue(param, "size", Integer.MAX_VALUE);

            if (param != null && param.get("aggregations") != null) {  //复杂聚合
                TermsAggregationBuilder termsAggregationBuilder = (TermsAggregationBuilder) param.get("aggregations");
                searchRequestBuilder.setSize(MixedConstant.INT_0);
                searchRequestBuilder.setFetchSource(false);
                termsAggregationBuilder.size(size);
                searchRequestBuilder.addAggregation(termsAggregationBuilder);
                if (log.isDebugEnabled()) {
                    log.debug("findListBySql searchRequestBuilder:" + searchRequestBuilder.toString());
                }
                SearchResponse searchResponse = searchRequestBuilder.get();
                param.put("aggregationResult", searchResponse.getAggregations());
                return Collections.emptyList();
            } else {    //正常查询没有聚合
                queryOverloadProtect(searchRequestBuilder, from, size, queryBuilder);
                return nonScrollQuery(searchRequestBuilder);
            }
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    private List<T> nonScrollQuery(SearchRequestBuilder searchRequestBuilder) {
        if (log.isDebugEnabled()) {
            log.debug("nonScrollQuery searchRequestBuilder:" + searchRequestBuilder.toString());
        }
        SearchResponse searchResponse;
        try {
            searchResponse = searchRequestBuilder.get();
        } catch (ElasticsearchException e) {
            throw ElasticSearchHelper.translateElasticSearchException(e);
        }
        return ElasticSearchHelper.getEntityList(searchResponse, entityClass, hasEsVersionFiled);
    }

    @Override
    public int insert(T entity) throws DaoException {
        DaoHelper.checkArgumentEntity(entity);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);

            IdEntity idEntity = (IdEntity) entity;
            Field pkField = DaoHelper.getPkField(idEntity);
            Object pkValue = DaoHelper.getColumnValue(pkField, idEntity);
            boolean hasSetPkValue = DaoHelper.hasSetPkValue(pkValue);

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type);
            if (hasSetPkValue) {
                indexRequestBuilder.setId(pkValue.toString()).setCreate(true);
            }

            String sourceJsonStr = ElasticSearchHelper.getSourceJsonStrWhenInsert(entity, hasEsVersionFiled, notNeedTransientPropertyList);
            indexRequestBuilder.setSource(sourceJsonStr, XContentType.JSON);

            IndexResponse indexResponse = indexRequestBuilder.get();

            /**
             * 插入完成后把es自动生成的id设置回entity
             */
            if (!hasSetPkValue) {
                String idAfterInsert = indexResponse.getId();
                DaoHelper.setColumnValue(pkField, idEntity, idAfterInsert);
            }

            /**
             * 插入完成后把es的version设置到entity
             */
            long version = indexResponse.getVersion();
            ElasticSearchHelper.setEsVersion(entity, version, hasEsVersionFiled);
            return new Long(version).intValue();         //新创建的文档版本都从1开始
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public int update(T entity) throws DaoException {
        DaoHelper.checkArgumentEntity(entity);

        return this.update(entity, null);
    }

    @Override
    public int update(T entity, List<String> propetyList) throws DaoException {
        DaoHelper.checkArgumentEntity(entity);

        IdEntity idEntity = (IdEntity) entity;
        Serializable pkValue = DaoHelper.getPkValue(idEntity);
        return this.updateById(pkValue, DaoHelper.entity2Update(entity, propetyList));
    }

    @Override
    public int updateById(Serializable id, Update update) throws DaoException {
        DaoHelper.checkArgumentId(id);
        DaoHelper.checkArgumentUpdate(update);

        Long oldVersion;
        try {
            //带版本更新
            oldVersion = (Long) update.get(ElasticSearchHelper.ES_VERSION_FIELD_NAME);
            if (oldVersion == null || oldVersion.longValue() <= MixedConstant.LONG_0) {
                oldVersion = Versions.MATCH_ANY;
            }
        } catch (RuntimeException e) {
            throw new DaoException("The id[" + id + "] use version update, value must be long type[" + update.toString() + "]");
        }

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate()
                    .setIndex(index)
                    .setType(type)
                    .setId(ElasticSearchHelper.getIdSerializable(id))
                    .setVersion(oldVersion)
                    .setDoc(ElasticSearchHelper.getSourceJsonStrWhenUpdate(update, hasEsVersionFiled, notNeedTransientPropertyList), XContentType.JSON);


            UpdateResponse updateResponse = updateRequestBuilder.get();
            int op = updateResponse.getResult().getOp();
            if (op == DocWriteResponse.Result.NOOP.getOp()) {   //值没有变化,_version不会增加
                return MixedConstant.INT_0;
            }
            return MixedConstant.INT_1;
        } catch (VersionConflictEngineException e) {
            if (oldVersion == Versions.MATCH_ANY) {
                this.refresh();
                return this.updateById(id, update);
            } else {
                throw DaoExceptionTranslator.translate(e);
            }
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    private void refresh() {
        long start = System.currentTimeMillis();
        Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
        RefreshResponse response = client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
        long time = System.currentTimeMillis() - start;
        if (response.getShardFailures().length == response.getTotalShards()) {
            log.info("refresh index[" + index + "] failed[" + response.getShardFailures() + "], time:" + time);
        } else if (response.getShardFailures().length > 0) {
            log.info("refresh index[" + index + "] part failed[" + response.getShardFailures() + "], time:" + time);
        }
        log.info("refresh index[" + index + "] success, time:" + time);
    }

    @Override
    public int updateByIds(List<Serializable> ids, Update update) throws DaoException {
        DaoHelper.checkArgumentIds(ids);
        DaoHelper.checkArgumentUpdate(update);

        if (ids.size() > ElasticSearchHelper.MAX_UPDATE_SIZE) {
            throw new DaoMethodParameterException("单次更新的记录多于" + ElasticSearchHelper.MAX_UPDATE_SIZE + "拒绝批量更新");
        }

        int count = MixedConstant.INT_0;
        for (Serializable id : ids) {
            int n = this.updateById(id, update);
            count += n;
        }
        return count;
    }

    @Override
    public int updateByCriteria(Criteria criteria, Update update) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentUpdate(update);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            QueryBuilder queryBuilder = ElasticSearchHelper.criteria2QueryBuilder(criteria);

            if (log.isDebugEnabled()) {
                log.debug("updateByCriteria queryBuilder:" + queryBuilder.toString());
            }

            int from = MixedConstant.INT_0;
            int size = new Long(countByCriteria(criteria)).intValue();
            if (size > ElasticSearchHelper.MAX_UPDATE_SIZE) {
                throw new DaoException("方法updateByCriteria查询条件[" + queryBuilder.toString() + "]命中文档多于" + ElasticSearchHelper.MAX_UPDATE_SIZE + "拒绝批量更新");
            }

            SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(false)
                    .setFrom(from)
                    .setSize(size);

            if (queryBuilder != null) {
                searchRequestBuilder.setQuery(queryBuilder);
            }
            if (log.isDebugEnabled()) {
                log.debug("updateByCriteria searchRequestBuilder:" + searchRequestBuilder.toString());
            }
            SearchResponse searchResponse = searchRequestBuilder.get();
            SearchHits searchHits = searchResponse.getHits();
            if (searchHits.getTotalHits() == MixedConstant.LONG_0) {
                return MixedConstant.INT_0;
            }

            List<Serializable> ids = Lists.newArrayList();
            for (SearchHit searchHit : searchHits.getHits()) {
                String id = searchHit.getId();
                ids.add(id);
            }
            return this.updateByIds(ids, update);
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public int updateBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        throw new DaoException("ElasticSearchBaseDao do not support The Method");
    }

    @Override
    public int deleteById(Serializable id) throws DaoException {
        DaoHelper.checkArgumentId(id);

        try {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            DeleteResponse deleteResponse = client.prepareDelete()
                    .setIndex(index)
                    .setType(type)
                    .setId(ElasticSearchHelper.getIdSerializable(id))
                    .get();

            int op = deleteResponse.getResult().getOp();
            if (op == DocWriteResponse.Result.NOT_FOUND.getOp()) {
                return MixedConstant.INT_0;
            }
            return MixedConstant.INT_1;
        } catch (RuntimeException e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOne(List<String> fields, Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findOneByQuery(query);
    }

    @Override
    public T findOne(Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        return this.findOneByQuery(query);
    }

    @Override
    public List<T> findList(List<String> fields, Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findList(List<String> fields, Criteria criteria, List<OrderBy> orderBys, Pageable pageable) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query(criteria);
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    @Override
    public List<T> findList(Criteria criteria) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);

        Query query = Query.query(criteria);
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findList(Criteria criteria, List<OrderBy> orderBys) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query(criteria);
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findList(Criteria criteria, List<OrderBy> orderBys, Pageable pageable) throws DaoException {
        DaoHelper.checkArgumentCriteria(criteria);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query(criteria);
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    @Override
    public List<T> findAllList() throws DaoException {
        Query query = Query.query();
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findAllList(List<String> fields) throws DaoException {
        DaoHelper.checkArgumentFields(fields);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findAllList(List<String> fields, List<OrderBy> orderBys) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentOrderBys(orderBys);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query);
    }

    @Override
    public List<T> findAllList(List<String> fields, List<OrderBy> orderBys, Pageable pageable) throws DaoException {
        DaoHelper.checkArgumentFields(fields);
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query();
        query.includeField(fields.toArray(new String[fields.size()]));
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    @Override
    public List<T> findAllList(List<OrderBy> orderBys, Pageable pageable) throws DaoException {
        DaoHelper.checkArgumentOrderBys(orderBys);
        DaoHelper.checkArgumentPageable(pageable);

        Query query = Query.query();
        query.orderBy(orderBys.toArray(new OrderBy[orderBys.size()]));
        return this.findListByQuery(query, pageable);
    }

    /**
     * 查询过载保护
     */
    private void queryOverloadProtect(SearchRequestBuilder searchRequestBuilder, int from, int size, QueryBuilder queryBuilder) {
        searchRequestBuilder.setFrom(from);
        searchRequestBuilder.setSize(size);
        if (size == Integer.MAX_VALUE) {
            Client client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequestBuilder countSearchBuilder = client.prepareSearch()
                    .setIndices(index)
                    .setTypes(type)
                    .setFetchSource(false)
                    .setFrom(MixedConstant.INT_0)
                    .setSize(MixedConstant.INT_0);    //es自动转换为count模式
            if (queryBuilder != null) {
                countSearchBuilder.setQuery(queryBuilder);
            }
            SearchResponse searchResponse = countSearchBuilder.get();
            long actualSize = searchResponse.getHits().getTotalHits();
            if (actualSize >= Integer.MAX_VALUE) {
                throw new DaoException("此次查询命中文档数已经大于Integer.MAX_VALUE,拒绝查询,查询条件searchRequestBuilder[" + searchRequestBuilder.toString() + "]");
            }
            searchRequestBuilder.setSize(Long.valueOf(actualSize).intValue());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    protected void afterPropertiesSet() {
        Class daoClass = this.getClass();
        //得到泛型entityClass
        ParameterizedType type = (ParameterizedType) daoClass.getGenericSuperclass();
        Type[] p = type.getActualTypeArguments();
        this.entityClass = (Class<T>) p[0];
        ElasticSearchHelper.checkEntityClass(entityClass);

        this.hasEsVersionFiled = ElasticSearchHelper.hasEsVersionField(entityClass);
        this.index = ElasticSearchHelper.getIndexName(entityClass);
        this.type = ElasticSearchHelper.getTypeName(entityClass);

        //得到jdbcSettings
        String settingsName = DaoHelper.getSettingsName(daoClass);
        this.elasticSearchSettings = (ElasticSearchSettings) this.applicationContext.getBean(settingsName);
        if (this.elasticSearchSettings == null) {
            throw new DaoException("注解Dao的属性settingBeanName[" + settingsName + "]必须对应一个有效的ElasticSearchSettings bean");
        }

        ElasticSearchClientFactory.INSTANCE.setClient(elasticSearchSettings);

        //设置不需要持久化的字段
        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            if (DaoHelper.isFinalOrStatic(field)) {
                continue;
            }
            String propertyName = field.getName();
            if (field.getAnnotation(PK.class) != null) {
                pkFieldName = propertyName;
            }
            com.github.zhouyutong.zorm.dao.elasticsearch.annotation.Field annotation = field.getAnnotation(com.github.zhouyutong.zorm.dao.elasticsearch.annotation.Field.class);
            if (!annotation.isTransient()) {
                notNeedTransientPropertyList.add(propertyName);
            }
        }
    }
}
