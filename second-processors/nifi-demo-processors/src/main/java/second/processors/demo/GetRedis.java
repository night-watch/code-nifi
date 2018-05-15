package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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
 * Created by NightWatch on 2018/3/9.
 */
public class GetRedis extends AbstractProcessor {

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

    public static final PropertyDescriptor BEGIN = new PropertyDescriptor.Builder()
            .name("list begin index").displayName("list begin index")
            .description("当type为list时，要取下标的起始值（ 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推），具体的参照redis API")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(true).build();

    public static final PropertyDescriptor END = new PropertyDescriptor.Builder()
            .name("list end index").displayName("list end index")
            .description("当type为list时，要取下标的结束值（也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推），具体的参照redis API")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(true).build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields").displayName("fields").description("当type为map时，用来取值得field的值，为空时则取全部的field")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).build();

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
        descriptors.add(BEGIN);
        descriptors.add(END);
        descriptors.add(FIELDS);
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
            String type = context.getProperty(TYPE).getValue();

            String key = context.getProperty(KEY).getValue();

            JSONObject jsonObject = null;

            switch (type) {
                case "set" :
                    jsonObject = set(key);
                    break;
                case "key-value" :
                    jsonObject = keyValue(key);
                    break;
                case "list" :
                    Integer begin = context.getProperty(BEGIN).asInteger();
                    Integer end = context.getProperty(END).asInteger();
                    jsonObject = list(key, begin, end);
                    break;
                case "map" :
                    String field = context.getProperty(FIELDS).getValue();
                    jsonObject = map(key, field);
                    break;
            }

            final JSONObject finalJ = jsonObject;

            FlowFile write = session.write(flowFile, out -> {
                out.write(finalJ.toString().getBytes());
            });

            session.transfer(write, SUCCESS);
        } catch (Exception e) {
            getLogger().error("GetRedis", e);
            session.transfer(flowFile, FAILURE);
        }

    }


    private JSONObject set(String key) {
        Set<String> set = redis.smembers(key);
        JSONArray jsonArray = JSONArray.fromObject(set);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key, jsonArray);
        return jsonObject;
    }

    private JSONObject keyValue(String key) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key, redis.get(key));
        return jsonObject;
    }

    private JSONObject list(String key, Integer start, Integer end) {
        List<String> list = redis.lrange(key, start, end);
        JSONArray jsonArray = JSONArray.fromObject(list);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(key, jsonArray);
        return jsonObject;
    }

    private JSONObject map(String key, String field) {
        JSONObject jsonObject = new JSONObject();
        if (field == null || field.length() == 0) {
            jsonObject.put(key, JSONObject.fromObject(redis.hgetAll(key)));
            return jsonObject;
        }
        String[] fields = field.split(",");
        Map<String, String> map = new HashedMap();
        for (int i = 0; i < fields.length; i++) {
            String value = redis.hget(key, fields[i]);
            map.put(fields[i], value);
        }
        jsonObject.put(key, JSONObject.fromObject(map));
        return jsonObject;
    }
}
