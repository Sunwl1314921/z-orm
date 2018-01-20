package com.github.zhouyutong.zorm.entity;


import com.github.zhouyutong.zorm.annotation.Column;
import com.github.zhouyutong.zorm.constant.DBConstant;
import lombok.Getter;
import lombok.Setter;

/**
 * 基于long主键id的所有实体对象基类
 *
 * @author zhouyutong
 * @version 0.0.1
 * @since 2015/11/24
 */
@Getter
@Setter
public abstract class LongIdEntity implements IdEntity {
    @Column(DBConstant.PK_NAME)
    protected Long id;

    @Override
    public abstract String toString();
}
