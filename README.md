# z-orm
z-orm框架是非常易用的orm框架，基于spring-jdbc、elasticsearch之上，对service层暴露统一的单表增删改查操作。
支持：
1、面向对象方式构造查询语句，而与具体的数据源无关。
2、单表的查询语句除了OR不用显示写SELECT语句。
3、不支持的SQL可通过显示传递满足任何查询。
4、支持本地事物内查询的强一致。
5、目前支持的关系数据库包括MYSQL,ORACLE。
6、目前支持的NOSQL包括elasticsearch。
