package second.processors.demo;

import nifi.bicon.util.Constant;
import org.apache.commons.collections.map.HashedMap;
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
import java.util.*;
import java.util.Date;

/**
 * Created by NightWatch on 2018/2/7.
 */
public class NewsMysqlToRedis extends AbstractProcessor {

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
    }

    @OnUnscheduled
    public void unscheduled() {
        jedis.close();
    }



    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        try {
            Statement mySQLStatement = connection.createStatement();

            long timeStamp = (timeStamp() - 3 * 60 * 60 * 24 * 1000) / 1000;

            String sql = "SELECT id, title, image_url, type, tags, src, writer, url, read_count, comment_count, collection_count, UNIX_TIMESTAMP(create_date), count, action_type_x FROM news_info WHERE UNIX_TIMESTAMP(create_date) > " + timeStamp;
            ResultSet resultSet = mySQLStatement.executeQuery(sql);
            while (resultSet.next()) {
                String id = resultSet.getString(1);
                String title = resultSet.getString(2);
                String image_url = resultSet.getString(3);
                String type = resultSet.getString(4);
                String tags = resultSet.getString(5);
                String src = resultSet.getString(6);
                String writer = resultSet.getString(7);
                String url = resultSet.getString(8);
                String read_count = resultSet.getString(9);
                String comment_count = resultSet.getString(10);
                String collection_count = resultSet.getString(11);
                long create_date = resultSet.getLong(12) * 1000;
                String all_count = resultSet.getString(13);
                String action_type_x = resultSet.getString(14);
                String newsType = "2";
                if (image_url.length() == 0) {
                    newsType = "0";
                } else if (image_url.split(",").length < 3) {
                    newsType = "1";
                }
                Map<String, String> newsInfo = new HashedMap();
                newsInfo.put("id", id);
                newsInfo.put("title", title);
                newsInfo.put("image_url", image_url);
                newsInfo.put("type", type);
                newsInfo.put("tags", tags);
                newsInfo.put("src", src);
                newsInfo.put("writer", writer);
                newsInfo.put("url", url);
                newsInfo.put("read_count", read_count);
                newsInfo.put("comment_count", comment_count);
                newsInfo.put("collection_count", collection_count);
                newsInfo.put("create_date", create_date + "");
                newsInfo.put("all_count", all_count);
                newsInfo.put("action_type_x", action_type_x);
                newsInfo.put("newsType", newsType);

                int x = Integer.parseInt(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

                Set<String> newsIDs = jedis.hkeys(Constant.NEWID_TO_ORDERID);

                if (!newsIDs.contains(id)) {

                    jedis.hmset(Constant.NEWS_ORDER_TODAY_ + x, newsInfo);

                    jedis.hset(Constant.NEWID_TO_ORDERID, id, Constant.NEWS_ORDER_TODAY_ + x);

                    jedis.incrBy(Constant.NEWS_ORDER_TODAY_COUNT, 1);
                }
            }
            session.transfer(write(flowFile, true, session), MY_RELATIONSHIP_SUCEESS);
        } catch (SQLException e) {
            getLogger().error("NewsMysqlToRedis");
            session.transfer(write(flowFile, false, session), MY_RELATIONSHIP_FAILURE);
        } catch (Exception e) {
            getLogger().error("NewsMysqlToRedis");
            session.transfer(write(flowFile, false, session), MY_RELATIONSHIP_FAILURE);
        }
    }

    private FlowFile write(FlowFile flowFile, boolean success, ProcessSession session) {
        String content = success ? "SUCCESS" : "FAILURE";
        FlowFile write = session.write(flowFile, out -> {
            out.write(content.getBytes());
        });
        return write;
    }

    private long timeStamp() {
        int rawOffset = TimeZone.getDefault().getRawOffset();
        long timeStamp = (new Date().getTime() + rawOffset) / (3600 * 24 * 1000) * (3600 * 24 * 1000) - rawOffset;
        return timeStamp;
    }

}
