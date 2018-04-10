package com.zhouyutong.zorm.dao.elasticsearch;

import com.google.common.collect.Lists;
import com.zhouyutong.zorm.annotation.PK;
import com.zhouyutong.zorm.constant.MixedConstant;
import com.zhouyutong.zorm.dao.DaoHelper;
import com.zhouyutong.zorm.dao.elasticsearch.annotation.Document;
import com.zhouyutong.zorm.entity.IdEntity;
import com.zhouyutong.zorm.exception.DaoException;
import com.zhouyutong.zorm.query.Criteria;
import com.zhouyutong.zorm.query.CriteriaOperators;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;


/**
 * @Author zhouyutong
 * @Date 2017/5/11
 */
public final class ElasticSearchHelper {
    public static final char COMMON_WILDCARD = '%';
    public static final char ES_WILDCARD = '*';

    private ElasticSearchHelper() {
    }

    /**
     * 根据criteria创建QueryBuilder
     *
     * @param criteria -
     * @return
     */
    public static QueryBuilder criteria2QueryBuilder(Criteria criteria) {
        if (criteria == null) {
            return QueryBuilders.matchAllQuery();
        }
        BoolQueryBuilder boolQueryBuilder = boolQuery();
        List<Criteria> criterias = criteria.getCriteriaChain();
        for (Criteria c : criterias) {
            String field = c.getKey();
            String operator = c.getOperator();
            Object value = c.getValue();

            if (CriteriaOperators.EQ.match(operator)) {
                boolQueryBuilder.must(termQuery(field, value));
            } else if (CriteriaOperators.GTE.match(operator)) {
                boolQueryBuilder.must(rangeQuery(field).gte(value));
            } else if (CriteriaOperators.GT.match(operator)) {
                boolQueryBuilder.must(rangeQuery(field).gt(value));
            } else if (CriteriaOperators.LTE.match(operator)) {
                boolQueryBuilder.must(rangeQuery(field).lte(value));
            } else if (CriteriaOperators.LT.match(operator)) {
                boolQueryBuilder.must(rangeQuery(field).lt(value));
            } else if (CriteriaOperators.NE.match(operator)) {
                boolQueryBuilder.mustNot(termQuery(field, value));
            } else if (CriteriaOperators.IN.match(operator)) {
                Collection collection = (Collection) value;
                boolQueryBuilder.must(termsQuery(field, collection));
            } else if (CriteriaOperators.NIN.match(operator)) {
                Collection collection = (Collection) value;
                boolQueryBuilder.mustNot(termsQuery(field, collection));
            } else if (CriteriaOperators.LIKE.match(operator)) {         //ES LIKE等同于not analyzed 的包含操作
                String v = (String) value;
                char firstChar = v.charAt(0);
                char lastChar = v.charAt(v.length() - 1);
                if (firstChar != COMMON_WILDCARD && lastChar == COMMON_WILDCARD) {
                    boolQueryBuilder.must(prefixQuery(field, v));
                } else {
                    boolQueryBuilder.must(wildcardQuery(field, v.replaceAll(Character.toString(COMMON_WILDCARD), Character.toString(ES_WILDCARD))));
                }
            } else if (CriteriaOperators.MATCH.match(operator)) {
                boolQueryBuilder.must(matchQuery(field, value));
            } else if (CriteriaOperators.MATCH_PHRASE.match(operator)) {
                boolQueryBuilder.must(matchPhraseQuery(field, value));
            }

        }
        return boolQueryBuilder;
    }

    static String[] includeFileds(List<String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return MixedConstant.EMPTY_STRING_ARRAY;
        }
        return fields.toArray(new String[fields.size()]);
    }

    static <T> List<T> getEntityList(SearchResponse searchResponse, Class<T> entityClass) {
        SearchHits searchHits = searchResponse.getHits();
        if (searchHits.getTotalHits() == MixedConstant.LONG_0) {
            return Collections.emptyList();
        }

        List<T> entityList = Lists.newArrayList();
        for (SearchHit searchHit : searchHits.getHits()) {
            String source = searchHit.getSourceAsString();
            entityList.add(FastJson.jsonStr2Object(source, entityClass));
        }
        return entityList;
    }

    static <T> T getEntity(SearchResponse searchResponse, Class<T> entityClass) {
        SearchHits searchHits = searchResponse.getHits();
        if (searchHits.getTotalHits() == MixedConstant.LONG_0) {
            return null;
        }

        SearchHit searchHit = searchHits.getHits()[MixedConstant.INT_0];
        String source = searchHit.getSourceAsString();
        return FastJson.jsonStr2Object(source, entityClass);
    }

    static DaoException translateElasticSearchException(Exception e) {
        /**
         * 按照惯例通常limit=0表示查询所有的,但es5默认根据index.max_result_window配置form+size不能超过10000
         * 所以这里要设置成很大的值让es抛出异常以便研发知道他没有得到期望的记录数
         * 官方提示：
         * {
         * "type": "query_phase_execution_exception",
         * "reason": "Result window is too large, from + size must be less than or equal to: [10000] but was [489308].
         * See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting."
         * }
         */
        StringBuilder sb = new StringBuilder();
        String errorMessage = e.toString();
        if (errorMessage.contains("max_result_window")) {
            sb.append("ElasticSearch查询结果限制[from + size不能超过index.max_result_window设置的值,默认是10000]");
            sb.append("官方错误:");
            sb.append(errorMessage);
        }
        throw new DaoException(sb.toString(), e);
    }

    public static String getIndexName(Class entityClass) {
        Document documentAnn = (Document) entityClass.getAnnotation(Document.class);
        return documentAnn.indexName();
    }

    public static String getTypeName(Class entityClass) {
        Document documentAnn = (Document) entityClass.getAnnotation(Document.class);
        return documentAnn.typeName();
    }

    /**
     * 校验entityClass必须符合框架的规范
     *
     * @param entityClass
     */
    public static void checkEntityClass(Class entityClass) {
        if (entityClass == null) {
            throw new DaoException("can not get the entity's Generic Type");
        }

        String entityClassName = entityClass.getName();
        if (!IdEntity.class.isAssignableFrom(entityClass)) {
            throw new DaoException("entity[" + entityClassName + "] must implements IdEntity");
        }

        Document tableAnnotation = (Document) entityClass.getAnnotation(Document.class);
        if (tableAnnotation == null) {
            throw new DaoException("entity[" + entityClassName + "] must have Document annotation");
        }

        Field[] fields = entityClass.getDeclaredFields();
        if (fields == null || fields.length == 0) {
            throw new DaoException("entity[" + entityClassName + "] must have least one Field");
        }

        int pkAnnotationCount = 0;
        String pkFieldTypeName = "";
        List<String> supportPKFieldType = Lists.newArrayList("java.lang.Integer", "java.lang.Long", "java.lang.String");
        for (Field field : fields) {
            if (DaoHelper.isFinalOrStatic(field)) {
                continue;
            }
            com.zhouyutong.zorm.dao.elasticsearch.annotation.Field columnAnnotation = field.getAnnotation(com.zhouyutong.zorm.dao.elasticsearch.annotation.Field.class);
            if (columnAnnotation == null) {
                throw new DaoException("entity[" + entityClassName + "]的字段[" + field.getName() + "]必须有Column注解");
            }

            PK pkAnnotation = field.getAnnotation(PK.class);
            if (pkAnnotation != null) {
                pkAnnotationCount++;
                pkFieldTypeName = field.getType().getName();
            }
        }
        if (pkAnnotationCount != 1) {
            throw new DaoException("entity[" + entityClassName + "] 有且只能有一个PK注解的字段");
        }
        if (!supportPKFieldType.contains(pkFieldTypeName)) {
            throw new DaoException("entity[" + entityClassName + "]的pk字段类型只能是Long,Integer,String其中之一");
        }
    }
}
