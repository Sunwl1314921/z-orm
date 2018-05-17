package com.zhouyutong.zorm.dao.cassandra;

import com.zhouyutong.zorm.dao.DaoSettings;
import com.zhouyutong.zorm.enums.DialectEnum;
import lombok.Data;

import javax.sql.DataSource;
import java.util.List;

/**
 * Cassandra 通用client级别设置对象
 *
 * @Author zhouyutong
 * @Date 2017/5/16
 */
@Data
public class CassandraSettings implements DaoSettings {
    /**
     * 服务端node节点地址列表
     * 1.1.1.1:9300,1.1.1.9:9300
     */
    private String serverAddressList;
    /**
     * 集群名称
     */
    private String clusterName;
    private String userName;
    private String password;
}
