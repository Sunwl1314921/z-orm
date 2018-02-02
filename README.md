## z-orm框架是一款轻量易用的统一dao层框架，针对不同的底层数据源(关系数据库、Nosql数据库)提供统一的数据访问接口。<br>
## 支持功能：<br>
1、提供类MongoDB的Query和类Hibernate的面向对象方式查询。<br>
2、满足80%的单表查询需求而不需要显示写SQL语句，更不需要想mybatis那样讲SQL语句写在XML或方法参数注解上。<br>
3、不支持子查询、OR、JOIN等复杂SQL，但可通过显示传递sql语句满足任何查询。<br>
4、采用回调callback机制使用编程式事物，避免注解式事物由于代码规模不断膨胀导致事物粒度不断扩大的风险。<br>
5、目前支持的关系数据库包括MYSQL,ORACLE。<br>
6、目前支持的NOSQL包括elasticsearch，未来将计划支持MongoDB、Cassandra 和 HBase。<br>
7、支持TCC分布式事物。<br>

## Use
####1、添加jar依赖
```xml
<dependency>
    <groupId>com.github.zhouyutong</groupId>
    <artifactId>z-orm-elasticsearch</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>com.github.zhouyutong</groupId>
    <artifactId>z-orm-jdbc</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>

<repositories>
    <repository>
        <id>zhouyutong-mvn-repo-snapshot</id>
        <url>https://raw.github.com/zhouyutong/maven-repo/snapshot/</url>
    </repository>
    <repository>
        <id>zhouyutong-mvn-repo-release</id>
        <url>https://raw.github.com/zhouyutong/maven-repo/release/</url>
    </repository>
</repositories>
```
####2、配置事物管理（可选）,配置daoSettingBean

# 作者联系：qq101109677
