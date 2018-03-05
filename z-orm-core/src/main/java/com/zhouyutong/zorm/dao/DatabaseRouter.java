package com.zhouyutong.zorm.dao;

/**
 * 数据源路由器标识
 *
 * @Author zhouyutong
 * @Date 2017/6/8
 */
public interface DatabaseRouter {

    Object writeRoute();

    Object readRoute();
}
