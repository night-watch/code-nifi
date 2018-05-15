package second.processors.demo;

import net.sf.json.JSONObject;
import nifi.bicon.util.Constant;
import nifi.bicon.util.JedisClusterUtil;
import nifi.bicon.util.JedisCreater;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import second.serviceDemo.JedisSentinelPoolService;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/12.
 * 将新闻信息传入redis中
 */
public class NewsInfoToRedis extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_POOL = new PropertyDescriptor.Builder()
            .name("redis connection pool").description("redis连接池")
            .identifiesControllerService(JedisSentinelPoolService.class)
            .required(true).build();

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

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_POOL);
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

    @OnScheduled
    public void scheduled(ProcessContext context) {
        JedisSentinelPoolService pool = context.getProperty(REDIS_POOL).asControllerService(JedisSentinelPoolService.class);
        jedis = pool.getJedis();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final byte[] buffer = new byte[(int)flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                int bytesRead = 0;
                int len;
                while (bytesRead < buffer.length) {
                    len = in.read(buffer, bytesRead, buffer.length - bytesRead);
                    if (len < 0) {
                        throw new EOFException();
                    }
                    bytesRead += len;
                }
            }
        });
        try {
            String content = new String(buffer, StandardCharsets.UTF_8);

            JSONObject jsonObject = JSONObject.fromObject(content);

            Integer newsOrderTodayCount = Integer.parseInt(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

            Map<String, String> newsInfo = new HashedMap();

            String imageUrl = jsonObject.getString("image_url");

            String newsType = "2";

            if (imageUrl.length() == 0) {
                newsType = "0";
            } else if (imageUrl.split(",").length < 3) {
                newsType = "1";
            }

            String id = jsonObject.getString("id");

            newsInfo.put("comment_count", jsonObject.getString("comment_count"));
            newsInfo.put("src", jsonObject.getString("src"));
            newsInfo.put("title", jsonObject.getString("title"));
            newsInfo.put("type", jsonObject.getString("type"));
            newsInfo.put("url", jsonObject.getString("url"));
            newsInfo.put("tags", jsonObject.getString("tags"));
            newsInfo.put("action_type_x", jsonObject.getString("action_type_x"));
            newsInfo.put("collection_count", jsonObject.getString("collection_count"));
            newsInfo.put("image_url", imageUrl);
            newsInfo.put("news_type", newsType);
            newsInfo.put("id", id);
            newsInfo.put("writer", jsonObject.getString("writer"));
            newsInfo.put("create_date", jsonObject.getString("create_date"));
            newsInfo.put("read_count", jsonObject.getString("read_count"));
            newsInfo.put("all_count", "0");
            Set<String> ids = jedis.hkeys(Constant.NEWID_TO_ORDERID);
            if (!ids.contains(id)) {
                jedis.hmset(Constant.NEWS_ORDER_TODAY_ + newsOrderTodayCount, newsInfo);
                jedis.hset(Constant.NEWID_TO_ORDERID, jsonObject.getString("id"), Constant.NEWS_ORDER_TODAY_ + newsOrderTodayCount);
                newsOrderTodayCount ++;
                jedis.set(Constant.NEWS_ORDER_TODAY_COUNT, String.valueOf(newsOrderTodayCount));
            }
            session.transfer(flowFile, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("错误", e);
            session.transfer(flowFile, MY_RELATIONSHIP_FAILURE);
        }

    }

    /**
     * 关闭资源
     */
    @OnUnscheduled
    public void unScheduled() {

        jedis.close();
    }
}
