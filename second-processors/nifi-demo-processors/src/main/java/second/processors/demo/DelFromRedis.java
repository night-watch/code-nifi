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
 * Created by NightWatch on 2018/1/15.
 */
public class DelFromRedis extends AbstractProcessor {

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

    @OnUnscheduled
    public void  unscheduled() {
        jedis.close();
    }


    /**
     * 每天夜里12点执行，将redis中三天内的新闻全部取出遍历，如果时间超过三天即(1000*60*60*24*3)带上毫秒值，则从redis中删除
     * 且将当天的新闻全部取出，然后删除当天新闻的map，将新闻放入三天新闻map中
     * @param context
     * @param session
     * @throws ProcessException
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        try {

            Map<String, String> oldNewsNow = jedis.hgetAll(Constant.OLD_NEWS_NOW);

            Integer length = Integer.valueOf(oldNewsNow.get("length"));

            String nowOrderName = oldNewsNow.get(Constant.ORDER_NOW_NAME);

            String newOrderName = oldNewsNow.get(Constant.ORDER_NEW_NAME);

            String removeIDs = ",";

            Integer removeLen = 0;

            for (int i = 0; i < length; i++) {
                long createDate = Long.parseLong(jedis.hget(nowOrderName + i, "create_date"));

                if (new Date().getTime() - createDate >= 1000 * 60 * 60 * 24 * 3) {
                    removeIDs = removeIDs + i + ",";
                    removeLen ++;
                }

            }

            long[] orderCounts = new long[length];

            long[] indexs = new long[length];

            for (int i = 0; i < length; i ++) {

                if (removeIDs.contains("," + i + ",")) {
                    orderCounts[i] = -1;
                } else {
                    orderCounts[i] = jedis.hincrBy(nowOrderName + i, "all_count", 0);
                }
                indexs[i] = i;
            }

            for (int i = 0; i < length; i ++) {
                for (int j = 0; j < length - 1 - i; j ++) {
                    if (orderCounts[j] < orderCounts[j + 1]) {
                        long temp = orderCounts[j];
                        orderCounts[j] = orderCounts[j + 1];
                        orderCounts[j + 1] = temp;

                        temp = indexs[j];
                        indexs[j] = indexs[j + 1];
                        indexs[j + 1] = temp;
                    }
                }
            }

            int len = length - removeLen;

            //
            for (int i = 0; i < length; i ++) {
                if (i <= len) {
                    jedis.hmset(newOrderName + i, jedis.hgetAll(nowOrderName + indexs[i]));
                    jedis.hset(Constant.NEWID_TO_ORDERID, jedis.hget(nowOrderName + indexs[i], "id"), i + "");
                }
                jedis.del(nowOrderName + indexs[i]);
            }

            Integer newsOrderTodayCount = Integer.parseInt(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

            for (int i = 0; i < newsOrderTodayCount; i++) {
                jedis.hmset(newOrderName + len, jedis.hgetAll(Constant.NEWS_ORDER_TODAY_ + i));
                jedis.del(Constant.NEWS_ORDER_TODAY_ + i);
                len = len + 1;
            }
            jedis.set(Constant.NEWS_ORDER_TODAY_COUNT, "0");


            oldNewsNow.put("length", len + "");

            oldNewsNow.put(Constant.ORDER_NOW_NAME, newOrderName);

            oldNewsNow.put(Constant.ORDER_NEW_NAME, nowOrderName);

            jedis.hmset(Constant.OLD_NEWS_NOW, oldNewsNow);

            Map<String, String> recommended = jedis.hgetAll("recommended");

            //如果recommendedMap不为空，则遍历，对每个userID下的三天前推荐的新闻删除
            if (recommended != null && recommended.size() != 0) {
                for (Map.Entry<String, String> entry : recommended.entrySet()){
                    String value = entry.getValue();
                    if (value == null) {
                        break;
                    }
                    String[] split = value.split("[+]");
                    //如果超过三天，则删除第一天的，并将所有字符串添加上"+,"，防止用户后期不看，数据还一直存在
                    if (split.length > 3) {
                        value = split[1] + "+" + split[2] + "+" + split[3] + "+,";
                    } else {
                        value = value + "+,";
                    }
                    jedis.hset("recommended", entry.getKey(), value);
                }
            }
            FlowFile write = session.write(flowFile, out -> {
                out.write("SUCCESS".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("每天晚上12点更新失败", e);
            session.transfer(flowFile, MY_RELATIONSHIP_FAILURE);
        }
    }
}
