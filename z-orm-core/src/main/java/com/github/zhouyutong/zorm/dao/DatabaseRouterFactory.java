package com.github.zhouyutong.zorm.dao;

/**
 * 数据源路由器工厂类
 *
 * @Author zhouyutong
 * @Date 2017/6/8
 */
public interface DatabaseRouterFactory {

    DatabaseRouter getDatabaseRouter(DaoSettings daoSettings);

    void setDatabaseRouter(DaoSettings daoSettings);
}
