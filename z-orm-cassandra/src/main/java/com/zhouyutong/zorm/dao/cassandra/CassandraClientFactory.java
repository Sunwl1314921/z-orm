package com.zhouyutong.zorm.dao.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import com.zhouyutong.zorm.constant.SymbolConstant;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * @Author zhouyutong
 * @Date 2017/5/17
 */
public final class CassandraClientFactory {
    static final CassandraClientFactory INSTANCE = new CassandraClientFactory();
    private HashMap<CassandraSettings, Session> clientMap = Maps.newHashMap();

    /**
     * 客户端的获取发生在项目运行中
     *
     * @param cassandraSettings
     */
    Session getClient(CassandraSettings cassandraSettings) {
        return clientMap.get(cassandraSettings);
    }

    /**
     * 客户端的创建工作发生在项目启动过程
     *
     * @param cassandraSettings
     */
    synchronized void setClient(CassandraSettings cassandraSettings) {
        if (this.getClient(cassandraSettings) != null) {
            return;
        }

        try {
            String[] serverAddrArr = cassandraSettings.getServerAddressList().split(SymbolConstant.COMMA);
            InetSocketAddress[] inetSocketAddressArr = new InetSocketAddress[serverAddrArr.length];
            for (int i = 0; i < serverAddrArr.length; i++) {
                String[] ipAndPort = serverAddrArr[i].split(SymbolConstant.COLON);
                inetSocketAddressArr[i] = new InetSocketAddress(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
            }
            Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(inetSocketAddressArr);
            if (StringUtils.isNotBlank(cassandraSettings.getUserName()) && StringUtils.isNotBlank(cassandraSettings.getPassword())) {
                builder.withCredentials(cassandraSettings.getUserName(), cassandraSettings.getPassword());
            }
            if (StringUtils.isNotBlank(cassandraSettings.getClusterName())) {
                builder.withClusterName(cassandraSettings.getClusterName());
            }
            Session session = builder.build().connect();
            clientMap.put(cassandraSettings, session);
        } catch (Exception e) {
            throw new RuntimeException("无法生产Client[" + cassandraSettings + "]", e);
        }
    }
}
