package second.processors.demo;

import nifi.bicon.util.Constant;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.util.*;

/**
 * Created by NightWatch on 2018/1/23.
 */
public class UserTarget extends AbstractProcessor {


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

        FlowFile flowFile = session.create();

        try {
            Set<String> smembers = jedis.smembers(Constant.USER_TARGET_SET);

            for (String userId : smembers) {
                jedis.del(Constant.USER_LRTZ + userId);
                jedis.srem(Constant.USER_TARGET_SET, userId);
            }


            FlowFile write = session.write(flowFile, out -> {
                out.write("SUCCESS".getBytes());
            });

            session.transfer(write, MY_RELATIONSHIP_SUCEESS);

        } catch (Exception e) {
            FlowFile write = session.write(flowFile, out -> {
                out.write("FAILURE".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_FAILURE);
            getLogger().error("每日删除用户健康指标项异常", e);
        }
    }
}
