package com.zhouyutong.zorm.dao.elasticsearch;

import com.zhouyutong.zorm.annotation.PK;
import com.zhouyutong.zorm.constant.MixedConstant;
import com.zhouyutong.zorm.dao.AbstractBaseDao;
import com.zhouyutong.zorm.dao.DaoHelper;
import com.zhouyutong.zorm.entity.IdEntity;
import com.zhouyutong.zorm.exception.DaoException;
import com.zhouyutong.zorm.exception.DaoExceptionTranslator;
import com.zhouyutong.zorm.exception.DaoMethodParameterException;
import com.zhouyutong.zorm.query.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
 * 基于ElasticSearch6.2.3 RestHighLevelClient的Dao实现
 *
 * @author zhouyutong
 * @Date 2017/4/20
 */
@Slf4j
public abstract class ElasticSearchBaseDao<T> extends AbstractBaseDao<T> implements ApplicationContextAware {

    private ElasticSearchSettings elasticSearchSettings;
    private String index;
    private String type;
    private String pkFieldName;
    private Class<T> entityClass;
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
        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(ElasticSearchHelper.criteria2QueryBuilder(criteria))
                    .size(MixedConstant.INT_0);
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("countByCriteria searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return searchResponse.getHits().getTotalHits();
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public long countAll() throws DaoException {
        return countByCriteria(null);
    }

    @Override
    protected long countBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);
        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(sql))
                    .size(MixedConstant.INT_0);
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("countBySql searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return searchResponse.getHits().getTotalHits();
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOneById(Serializable id) throws DaoException {
        DaoHelper.checkArgumentId(id);

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            GetRequest getRequest = new GetRequest(index, type, id.toString());
            GetResponse getResponse = client.get(getRequest);

            if (!getResponse.isExists()) {
                return null;
            }
            String source = getResponse.getSourceAsString();
            return FastJson.jsonStr2Object(source, entityClass);
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public T findOneByQuery(Query query) throws DaoException {
        DaoHelper.checkArgumentQuery(query);

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            String[] includes = ElasticSearchHelper.includeFileds(query.getFields());
            String[] excludes = MixedConstant.EMPTY_STRING_ARRAY;

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(ElasticSearchHelper.criteria2QueryBuilder(query.getCriteria()))
                    .fetchSource(includes, excludes)
                    .from(MixedConstant.INT_0)
                    .size(MixedConstant.INT_1);
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("findOneByQuery searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return ElasticSearchHelper.getEntity(searchResponse, entityClass);
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    protected T findOneBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.wrapperQuery(sql))
                    .from(MixedConstant.INT_0)
                    .size(MixedConstant.INT_1);
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("findOneBySql searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return ElasticSearchHelper.getEntity(searchResponse, entityClass);
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public List<T> findListByIds(List<Serializable> ids) throws DaoException {
        DaoHelper.checkArgumentIds(ids);
        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
            idsQueryBuilder.addIds(ids.toArray(new String[ids.size()]));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(idsQueryBuilder);
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("findListByIds searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return ElasticSearchHelper.getEntityList(searchResponse, entityClass);
        } catch (Exception e) {
            throw ElasticSearchHelper.translateElasticSearchException(e);
        }
    }

    @Override
    public List<T> findListByQuery(Query query) throws DaoException {
        DaoHelper.checkArgumentQuery(query);
        if (CollectionUtils.isNotEmpty(query.getGroupBys())) { //聚合使用findListBySql
            throw new DaoMethodParameterException("findListByQuery not support groupBy Search");
        }

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            String[] includes = ElasticSearchHelper.includeFileds(query.getFields());
            String[] excludes = MixedConstant.EMPTY_STRING_ARRAY;
            int from = query.getOffset() < MixedConstant.INT_0 ? MixedConstant.INT_0 : query.getOffset();
            int size = query.getLimit() < MixedConstant.INT_1 ? Integer.MAX_VALUE : query.getLimit();

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(ElasticSearchHelper.criteria2QueryBuilder(query.getCriteria()))
                    .fetchSource(includes, excludes)
                    .from(from)
                    .size(size);
            if (CollectionUtils.isNotEmpty(query.getOrderBys())) {
                for (OrderBy orderBy : query.getOrderBys()) {
                    String field = orderBy.getKey();
                    String direction = orderBy.getDirection();
                    SortOrder order = OrderBy.Direction.ASC.getDirection().equals(direction) ? SortOrder.ASC : SortOrder.DESC;
                    searchSourceBuilder.sort(field, order);
                }
            }
            searchRequest.source(searchSourceBuilder);

            if (log.isDebugEnabled()) {
                log.debug("findListByQuery searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);
            return ElasticSearchHelper.getEntityList(searchResponse, entityClass);
        } catch (Exception e) {
            throw ElasticSearchHelper.translateElasticSearchException(e);
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
    protected List<T> findListBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        DaoHelper.checkArgument(sql);
        if (param == null) {
            throw new DaoMethodParameterException("Param param must be not null");
        }

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            searchRequest.types(type);


            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            String aggKey = "AggregationBuilder";
            if (param.containsKey(aggKey)) {    //有聚合
                AggregationBuilder aggregationBuilder = (AggregationBuilder) param.get(aggKey);
                searchSourceBuilder.size(MixedConstant.INT_0);
                searchSourceBuilder.aggregation(aggregationBuilder);
            } else {    //无聚合
                int from = MapUtils.getIntValue(param, "from", MixedConstant.INT_0);
                int size = MapUtils.getIntValue(param, "size", Integer.MAX_VALUE);
                String[] includes = param.containsKey("includes") ? (String[]) param.get("includes") : MixedConstant.EMPTY_STRING_ARRAY;
                String[] excludes = param.containsKey("excludes") ? (String[]) param.get("excludes") : MixedConstant.EMPTY_STRING_ARRAY;
                searchSourceBuilder.query(QueryBuilders.wrapperQuery(sql))
                        .from(from)
                        .size(size)
                        .fetchSource(includes, excludes);
            }

            searchRequest.source(searchSourceBuilder);
            if (log.isDebugEnabled()) {
                log.debug("findListBySql searchRequestBuilder:" + searchSourceBuilder.toString());
            }
            SearchResponse searchResponse = client.search(searchRequest);

            if (param.containsKey(aggKey)) {    //有聚合
                param.put("AggregationResult", searchResponse.getAggregations());
                return Collections.emptyList();
            } else {   //无聚合
                return ElasticSearchHelper.getEntityList(searchResponse, entityClass);
            }
        } catch (Exception e) {
            throw ElasticSearchHelper.translateElasticSearchException(e);
        }
    }

    @Override
    public int insert(T entity) throws DaoException {
        DaoHelper.checkArgumentEntity(entity);

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);

            IdEntity idEntity = (IdEntity) entity;
            Field pkField = DaoHelper.getPkField(idEntity);
            Object pkValue = DaoHelper.getColumnValue(pkField, idEntity);
            boolean hasSetPkValue = DaoHelper.hasSetPkValue(pkValue);
            //使用es必须使用外部id
            if (!hasSetPkValue) {
                throw new DaoMethodParameterException("Param entity must be set id");
            }

            IndexRequest indexRequest = new IndexRequest(index, type);
            indexRequest.id(pkValue.toString());
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            String sourceJsonStr = FastJson.object2JsonStrUseNullValue(entity);
            indexRequest.source(sourceJsonStr, XContentType.JSON);
            IndexResponse indexResponse = client.index(indexRequest);
            long version = indexResponse.getVersion();
            return new Long(version).intValue();         //新创建的文档版本都从1开始
        } catch (Exception e) {
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

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            UpdateRequest request = new UpdateRequest(index, type, id.toString());
            request.doc(FastJson.object2JsonStrUseNullValue(update.getSetMap()), XContentType.JSON);
            request.retryOnConflict(3); //版本冲突重试3次
            request.docAsUpsert(false); //只更新

            UpdateResponse updateResponse = client.update(request);
            int op = updateResponse.getResult().getOp();
            if (op == DocWriteResponse.Result.NOOP.getOp()) {   //值没有变化,_version不会增加
                return MixedConstant.INT_0;
            }
            return MixedConstant.INT_1;
        } catch (Exception e) {
            throw DaoExceptionTranslator.translate(e);
        }
    }

    @Override
    public int updateByIds(List<Serializable> ids, Update update) throws DaoException {
        DaoHelper.checkArgumentIds(ids);
        DaoHelper.checkArgumentUpdate(update);

        int count = MixedConstant.INT_0;
        for (Serializable id : ids) {
            if (MixedConstant.INT_1 == this.updateById(id, update)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int updateByCriteria(Criteria criteria, Update update) throws DaoException {
        throw new DaoException("ElasticSearchBaseDao do not support The Method");
    }

    @Override
    protected int updateBySql(String sql, LinkedHashMap<String, Object> param) throws DaoException {
        throw new DaoException("ElasticSearchBaseDao do not support The Method");
    }

    @Override
    public int deleteById(Serializable id) throws DaoException {
        DaoHelper.checkArgumentId(id);

        try {
            RestHighLevelClient client = ElasticSearchClientFactory.INSTANCE.getClient(elasticSearchSettings);
            DeleteRequest deleteRequest = new DeleteRequest(index, type, id.toString());
            DeleteResponse deleteResponse = client.delete(deleteRequest);

            int op = deleteResponse.getResult().getOp();
            if (op == DocWriteResponse.Result.NOT_FOUND.getOp()) {
                return MixedConstant.INT_0;
            }
            return MixedConstant.INT_1;
        } catch (Exception e) {
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
        }
    }
}
