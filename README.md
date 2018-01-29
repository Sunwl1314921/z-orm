 z-orm<br>
z-orm框架是易用的统一dao层框架，对service层暴露统一的数据访问接口，而与具体的数据源无关，极大的将底层数据源访问复杂度和扩展收敛在dao层。<br>
支持功能：<br>
1、提供类MongoDB的Query和类Hibernate的面向对象方式查询。<br>
2、满足80%的单表查询需求而不需要显示写SQL语句，更不需要想mybatis那样讲SQL语句写在XML或方法参数注解上。<br>
3、不支持子查询、OR、JOIN等复杂SQL，但可通过显示传递sql语句满足任何查询。<br>
4、采用回调callback机制使用编程式事物，避免注解式事物由于代码规模不断膨胀导致事物粒度不断扩大的风险。<br>
5、目前支持的关系数据库包括MYSQL,ORACLE。<br>
6、目前支持的NOSQL包括elasticsearch，未来将计划支持MongoDB、Cassandra 和 HBase。<br>
7、支持TCC分布式事物。<br>
