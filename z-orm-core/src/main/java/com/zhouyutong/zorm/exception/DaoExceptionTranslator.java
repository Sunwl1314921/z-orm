package com.zhouyutong.zorm.exception;

/**
 * dao层异常转换器
 *
 * @Author zhouyutong
 * @Date 2017/6/20
 */
public class DaoExceptionTranslator {

    private DaoExceptionTranslator() {
    }

    public static DaoException translate(Throwable ex) {
        if (ex instanceof IllegalArgumentException) {
            return new DaoMethodParameterException("Dao Param Exception[" + ex.getMessage() + "]");
        } else if (ex instanceof DaoException) {
            return (DaoException) ex;
        } else {
            return new DaoException("Dao Unknown Exception[" + ex.getMessage() + "]", ex);
        }
    }
}
