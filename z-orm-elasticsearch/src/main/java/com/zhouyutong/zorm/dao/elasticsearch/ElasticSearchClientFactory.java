package com.zhouyutong.zorm.dao.elasticsearch;

import com.google.common.collect.Maps;
import com.zhouyutong.zorm.constant.SymbolConstant;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;

/**
 * @Author zhouyutong
 * @Date 2017/5/17
 */
public final class ElasticSearchClientFactory {
    static final ElasticSearchClientFactory INSTANCE = new ElasticSearchClientFactory();
    private HashMap<ElasticSearchSettings, RestHighLevelClient> transportClientMap = Maps.newHashMap();

    /**
     * 客户端的获取发生在项目运行中
     *
     * @param elasticSearchSettings
     */
    RestHighLevelClient getClient(ElasticSearchSettings elasticSearchSettings) {
        return transportClientMap.get(elasticSearchSettings);
    }

    /**
     * 客户端的创建工作发生在项目启动过程
     *
     * @param elasticSearchSettings
     */
    synchronized void setClient(ElasticSearchSettings elasticSearchSettings) {
        if (this.getClient(elasticSearchSettings) != null) {
            return;
        }

        try {
            String[] serverAddrArr = elasticSearchSettings.getServerAddressList().split(SymbolConstant.COMMA);
            HttpHost[] httpHost = new HttpHost[serverAddrArr.length];
            for (int i = 0; i < serverAddrArr.length; i++) {
                String[] ipAndPort = serverAddrArr[i].split(SymbolConstant.COLON);
                httpHost[i] = new HttpHost(ipAndPort[0], Integer.parseInt(ipAndPort[1]), "http");
            }
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost));
            transportClientMap.put(elasticSearchSettings, client);
        } catch (Exception e) {
            throw new RuntimeException("无法生产Client[" + elasticSearchSettings + "]", e);
        }
    }
}
