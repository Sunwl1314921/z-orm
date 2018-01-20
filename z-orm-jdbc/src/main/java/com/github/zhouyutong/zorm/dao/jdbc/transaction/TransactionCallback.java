package com.github.zhouyutong.zorm.dao.jdbc.transaction;

/**
 * 事务回调接口
 *
 * @author zhouyutong
 */
public interface TransactionCallback {

    /**
     * doTransaction中不要吞掉异常
     */
    Object doTransaction();
}
