package second.processors.demo;

import nifi.bicon.util.Constant;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Created by NightWatch on 2018/2/1.
 */
public class NewsToMySQL extends AbstractProcessor {


    public static final PropertyDescriptor REDIS_POOL = new PropertyDescriptor.Builder()
            .name("redis connection pool").description("redis连接池")
            .identifiesControllerService(JedisSentinelPoolService.class)
            .required(true).build();

    public static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool(Mysql)").description("数据库连接池")
            .required(true).identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table name").description("需要更新新闻的表名")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship MY_RELATIONSHIP_SUCEESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Example relationship")
            .build();
    public static final Relationship MY_RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Example relationship")
            .build();

    // 关系集合
    private Set<Relationship> relationships;

    // 自定义属性值列表
    private List<PropertyDescriptor> descriptors;

    private Jedis jedis;

    private DBCPService dbcpService;

    private Connection connection;

    private PreparedStatement preparedStatement;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_POOL);
        descriptors.add(CONNECTION_POOL);
        descriptors.add(TABLE_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP_SUCEESS);
        relationships.add(MY_RELATIONSHIP_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * 在start之前运行此方法，获取redis连接
     * @param context
     */
    @OnScheduled
    public void scheduled(ProcessContext context) throws SQLException {
        JedisSentinelPoolService pool = context.getProperty(REDIS_POOL).asControllerService(JedisSentinelPoolService.class);
        jedis = pool.getJedis();
        dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        connection = dbcpService.getConnection();
        String tableName = context.getProperty(TABLE_NAME).getValue();
        String sql = "replace INTO " + tableName + " (action_type_x, id, title, image_url, type, tags, src, writer, url, read_count, comment_count, collection_count, create_date, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), ?)";
        preparedStatement = connection.prepareStatement(sql);
    }

    /**
     * 在end时运行此方法，关闭redis连接，释放资源
     */
    @OnUnscheduled
    public void unscheduled() {
        try {
            jedis.close();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
            getLogger().error("更新新闻count中的Unscheduled异常", e);
            getLogger().info("\n\n\n" + Constant.BUGS);
        }

    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();

        try {

            getLogger().info(Constant.NO_BUG);



            Map<String, String> oldNewsNow = jedis.hgetAll(Constant.OLD_NEWS_NOW);

            int length = Integer.parseInt(oldNewsNow.get(Constant.LENGTH));

            String nowName = oldNewsNow.get(Constant.ORDER_NOW_NAME);

            int todayCount = Integer.parseInt(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

            for (int i = 0; i < length; i++) {
                Map<String, String> news = jedis.hgetAll(nowName + i);
                String commentCount = news.get("comment_count");
                String src = news.get("src");
                String title = news.get("title");
                String type = news.get("type");
                String url = news.get("url");
                String tags = news.get("tags");
                String actionTypeX = news.get("action_type_x");
                String collectionCount = news.get("collection_count");
                String imageUrl = news.get("image_url");
                String id = news.get("id");
                String writer = news.get("writer");
                java.sql.Date createDate = new Date(Long.parseLong(news.get("create_date")));
                String readCount = news.get("read_count");
                String allCount = news.get("all_count");
                preparedStatement.setString(1, actionTypeX);
                preparedStatement.setString(2, id);
                preparedStatement.setString(3, title);
                preparedStatement.setString(4, imageUrl);
                preparedStatement.setString(5, type);
                preparedStatement.setString(6, tags);
                preparedStatement.setInt(7, Integer.parseInt(src));
                preparedStatement.setString(8, writer);
                preparedStatement.setString(9, url);
                preparedStatement.setInt(10, Integer.parseInt(readCount));
                preparedStatement.setInt(11, Integer.parseInt(commentCount));
                preparedStatement.setInt(12, Integer.parseInt(collectionCount));
                preparedStatement.setDate(13, createDate);
                preparedStatement.setInt(14, Integer.parseInt(allCount));
                preparedStatement.execute();
            }

            for (int i = 0; i < todayCount; i++) {
                Map<String, String> news = jedis.hgetAll(Constant.NEWS_ORDER_TODAY_ + i);
                String commentCount = news.get("comment_count");
                String src = news.get("src");
                String title = news.get("title");
                String type = news.get("type");
                String url = news.get("url");
                String tags = news.get("tags");
                String actionTypeX = news.get("action_type_x");
                String collectionCount = news.get("collection_count");
                String imageUrl = news.get("image_url");
                String id = news.get("id");
                String writer = news.get("writer");
                java.sql.Date createDate = new Date(Long.parseLong(news.get("create_date")));
                String readCount = news.get("read_count");
                String allCount = news.get("all_count");
                preparedStatement.setString(1, actionTypeX);
                preparedStatement.setString(2, id);
                preparedStatement.setString(3, title);
                preparedStatement.setString(4, imageUrl);
                preparedStatement.setString(5, type);
                preparedStatement.setString(6, tags);
                preparedStatement.setInt(7, Integer.parseInt(src));
                preparedStatement.setString(8, writer);
                preparedStatement.setString(9, url);
                preparedStatement.setInt(10, Integer.parseInt(readCount));
                preparedStatement.setInt(11, Integer.parseInt(commentCount));
                preparedStatement.setInt(12, Integer.parseInt(collectionCount));
                preparedStatement.setDate(13, createDate);
                preparedStatement.setInt(14, Integer.parseInt(allCount));
                preparedStatement.execute();
            }
            FlowFile write = session.write(flowFile, out -> {
                out.write("SUCCESS".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (SQLException e) {
            getLogger().error("redis更新新闻至数据库异常", e);
            getLogger().info("\n\n\n" + Constant.BUGS);
            FlowFile write = session.write(flowFile, out -> {
                out.write("FAILURE".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_FAILURE);
        }

    }





}
