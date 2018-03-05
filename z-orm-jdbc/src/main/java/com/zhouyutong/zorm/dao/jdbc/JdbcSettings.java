package com.zhouyutong.zorm.dao.jdbc;

import com.zhouyutong.zorm.dao.DaoSettings;
import com.zhouyutong.zorm.dao.jdbc.enums.DialectEnum;
import lombok.Data;

import javax.sql.DataSource;
import java.util.List;

/**
 * jdbc 通用client级别设置对象
 *
 * @Author zhouyutong
 * @Date 2017/5/16
 */
@Data
public class JdbcSettings implements DaoSettings {
    private DialectEnum dialectEnum;
    private List<DataSource> writeDataSource;
    private List<DataSource> readDataSource;
}
