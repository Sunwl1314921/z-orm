## z-orm是一个轻量、易用的ORM框架，目标是为不同的底层数据源(关系数据库、Nosql数据库)提供统一的数据访问接口。<br>

## 支持功能：<br>
1、提供类MongoDB的Query和类Hibernate的面向对象方式查询。<br>
2、满足80%的单表查询需求而不需要显示写SQL语句，更不需要向mybatis那样将SQL语句写在XML或方法参数注解上。<br>
3、复杂SQL和大SQL在框架层面限制了只能在DAO层写，降低代码复杂度，提高可维护性。<br>
4、不支持子查询、OR、JOIN等复杂SQL，但可通过显示传递sql语句满足任何查询。<br>
5、采用回调callback机制使用编程式事物，避免注解式事物由于代码规模不断膨胀导致事物粒度不断扩大的风险。<br>
6、目前支持的关系数据库包括MYSQL,ORACLE。<br>
7、目前支持的NOSQL包括elasticsearch，未来将计划支持MongoDB、Cassandra 和 HBase。<br>

## QuickStart
#### 1、添加jar依赖
```xml
<!--使用elasticsearch引入-->
<dependency>
    <groupId>com.zhouyutong</groupId>
    <artifactId>z-orm-elasticsearch</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
<!--使用jdbc引入-->
<dependency>
    <groupId>com.zhouyutong</groupId>
    <artifactId>z-orm-jdbc</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
#### 2、配置事物管理（可选）,配置daoSettingBean
```java
@Bean(name = "jdbcSettings")
public JdbcSettings jdbcSettings_rrc(DataSource masterDataSource, DataSource slaveDataSource) {
    List<DataSource> masterDataSourceList = Lists.newArrayList(masterDataSource);
    List<DataSource> slaveDataSourceList = Lists.newArrayList(slaveDataSource);

    JdbcSettings jdbcSettings = new JdbcSettings();
    jdbcSettings.setDialectEnum(DialectEnum.MYSQL);
    jdbcSettings.setWriteDataSource(masterDataSourceList);
    jdbcSettings.setReadDataSource(slaveDataSourceList);
    return jdbcSettings;
}

@Bean(name = "transactionManager")
public TransactionManager transactionManager_rrc(DataSource masterDataSource) {
    PlatformTransactionManager platformTransactionManager = new DataSourceTransactionManager(masterDataSource);
    TransactionManager transactionManager = new TransactionManager();
    transactionManager.setTxManager(platformTransactionManager);
    return transactionManager;
}
```
#### 3、使用dao
```java
@Dao(settingBeanName = "jdbcSettings")
@Repository
public class MessageDao extends JdbcBaseDao<MessageEntity> {
}

@Service
public class MessageService {
    @Autowired
    private MessageDao messageDao;
    @Autowired
    private TransactionManager transactionManager;
    
    /**
     * 单表增删改查覆盖80%场景,剩余20%复杂sql通过find***BySql全覆盖
     */
    public void demo() {
        //findListBySql系列，受保护方法，只能dao内部调用
        messageDao.findListBySql(String sql);
        //findOneBySql系列，受保护方法，只能dao内部调用
        messageDao.findOneBySql(String sql);
        //findListByQuery系列
        messageDao.findListByQuery(Query query);
        //findOneByQuery系列
        messageDao.findOneByQuery(Query query);
        //findOne系列
        messageDao.findOne();
        //findList系列
        messageDao.findList();
         //update系列
        messageDao.update();
        
    }
}
```

# 作者联系：qq101109677
