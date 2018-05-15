package second.processors.demo;

import com.alibaba.fastjson.JSONObject;
import nifi.bicon.util.Constant;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.io.*;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/13.
 * 更新新闻排序标准count值
 */
public class UpdateRedisCount extends AbstractProcessor{

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

    /**
     * 在start之前运行此方法，获取redis连接
     * @param context
     */
    @OnScheduled
    public void scheduled(ProcessContext context) {
        JedisSentinelPoolService pool = context.getProperty(REDIS_POOL).asControllerService(JedisSentinelPoolService.class);
        jedis = pool.getJedis();
    }

    /**
     * 在end时运行此方法，关闭redis连接，释放资源
     */
    @OnUnscheduled
    public void unscheduled() {
        jedis.close();

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
            String content = new String(buffer);

            Map<String, String> map = (Map<String, String>) JSONObject.parse(content);

            String newsId = map.get("new_id");

            String type = String.valueOf(map.get("type"));

            String orderId = jedis.hget(Constant.NEWID_TO_ORDERID, newsId);

            if (!orderId.contains(Constant.NEWS_ORDER_TODAY_)) {
                orderId = jedis.hget(Constant.OLD_NEWS_NOW, Constant.ORDER_NOW_NAME) + orderId;
            }

            switch (type) {
                case "1" :
                    jedis.hincrBy(orderId, "all_count", 1);
                    jedis.hincrBy(orderId, "read_count", 1);
                    break;
                case "2" :
                    jedis.hincrBy(orderId, "all_count", 2);
                    jedis.hincrBy(orderId, "comment_count", 1);
                    break;
                case "3" :
                    jedis.hincrBy(orderId, "all_count", 3);
                    jedis.hincrBy(orderId, "collection_count", 1);
                    break;
                default:
                    throw new RuntimeException("type类型不匹配");
            }

            FlowFile write = session.write(flowFile, out -> {
                out.write("SUCCESS".getBytes());
            });

            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("更新redis中count", e);
            session.transfer(flowFile, MY_RELATIONSHIP_FAILURE);
        }
    }
}
