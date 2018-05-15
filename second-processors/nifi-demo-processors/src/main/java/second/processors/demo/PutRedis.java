package second.processors.demo;

import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.util.*;

/**
 * Created by NightWatch on 2018/3/8.
 */
public class PutRedis extends AbstractProcessor {

    public static final AllowableValue SET = new AllowableValue("set", "set");
    public static final AllowableValue KEY_VALUE = new AllowableValue("key-value", "key-value");
    public static final AllowableValue LIST = new AllowableValue("list", "list");
    public static final AllowableValue MAP = new AllowableValue("map", "map");

    public static final PropertyDescriptor REDIS_POOL = new PropertyDescriptor.Builder()
            .name("redis connection pool").displayName("redis connection pool")
            .description("redis连接池")
            .identifiesControllerService(JedisSentinelPoolService.class)
            .required(true).build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("data type").displayName("data type")
            .description("存储的数据类型").allowableValues(SET, KEY_VALUE, LIST, MAP)
            .required(true).build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("key").displayName("key").description("用来做key的值")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true).expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields").displayName("fields").description("当type为map时，用来做field的值，多个值之间用“,”分隔，且与VALUES一一对应")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).build();

    public static final PropertyDescriptor VALUES = new PropertyDescriptor.Builder()
            .name("values").displayName("values").description("用来做value的值，多个值之间用“,”分隔")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true).expressionLanguageSupported(true).build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success").description("All FlowFiles are routed to this Relationship when success.").build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("All FlowFiles are routed to this Relationship when failure.").build();

    // 自定义属性值列表
    private List<PropertyDescriptor> descriptors;

    // 关系集合
    private Set<Relationship> relationships;

    private Jedis redis;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REDIS_POOL);
        descriptors.add(TYPE);
        descriptors.add(KEY);
        descriptors.add(FIELDS);
        descriptors.add(VALUES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void scheduled(ProcessContext context) {
        JedisSentinelPoolService pool = context.getProperty(REDIS_POOL).asControllerService(JedisSentinelPoolService.class);
        redis = pool.getJedis();
    }

    @OnUnscheduled
    public void unscheduled() {
        redis.close();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        try {
            String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
            String values = context.getProperty(VALUES).evaluateAttributeExpressions(flowFile).getValue();
            String type = context.getProperty(TYPE).getValue();
            switch (type) {
                case "set" :
                    set(key, values);
                    break;
                case "key-value" :
                    keyValue(key, values);
                    break;
                case "list" :
                    list(key, values);
                    break;
                case "map" :
                    String fields = context.getProperty(FIELDS).evaluateAttributeExpressions(flowFile).getValue();
                    map(key, fields, values);
                    break;
            }
            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            getLogger().error("PutRedis", e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private void set(String key, String value) {
        String[] values = value.split(",");
        redis.sadd(key, values);
    }

    private void list(String key, String value) {
        String[] values = value.split(",");
        redis.lpush(key, values);
    }

    private void keyValue(String key, String value) {
        redis.set(key, value);
    }

    private void map(String key, String field, String value) throws Exception {
        String[] fields = field.split(",");
        String[] values = value.split(",");
        if (fields.length != values.length) {
            throw new Exception("field的个数和value的个数不一致");
        }
        Map<String, String> map = new HashedMap();
        for (int i = 0; i < fields.length; i++) {
            map.put(fields[i], values[i]);
        }
        redis.hmset(key, map);
    }
}