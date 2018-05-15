package second.processors.demo;

import nifi.bicon.util.BigMap;
import nifi.bicon.util.Constant;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/15.
 */
public class NewsSort extends AbstractProcessor {

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
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();

        try {
            //如果时间上和DelFromRedis冲突，则跳过这次排序
            SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
            String format = sdf.format(new Date());
            String[] split = format.split(":");
            if ((Integer.parseInt(split[0]) == 0 && Integer.parseInt(split[1]) < 25) || (Integer.parseInt(split[0]) == 23 && Integer.parseInt(split[1]) > 55) ) {
                FlowFile write = session.write(flowFile, out -> {
                    out.write("SUCCESS".getBytes());
                });
                session.transfer(write, MY_RELATIONSHIP_SUCEESS);
                return;
            }

            Map<String, String> oldNewsNow = jedis.hgetAll("old_news_now");

            Integer length = Integer.valueOf(oldNewsNow.get("length"));

            String nowOrderName = oldNewsNow.get(Constant.ORDER_NOW_NAME);

            String newOrderName = oldNewsNow.get(Constant.ORDER_NEW_NAME);

            long[] orderCounts = new long[length];

            long[] indexs = new long[length];

            for (int i = 0; i < length; i ++) {
                orderCounts[i] = jedis.hincrBy(nowOrderName + i, "all_count", 0);
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

            List<Map<String, String>> listOld = new ArrayList<>();


            for (int i = 0; i < length; i ++) {
                jedis.hmset(newOrderName + i, jedis.hgetAll(nowOrderName + indexs[i]));
                Map<String, String> map = new HashedMap();
                map.put("news_id", jedis.hget(newOrderName + i, "id"));
                map.put("title", jedis.hget(newOrderName + i, "title"));
                map.put("image_url", jedis.hget(newOrderName + i, "image_url"));
                map.put("read_count", jedis.hget(newOrderName + i, "read_count"));
                map.put("comment_count", jedis.hget(newOrderName + i, "comment_count"));
                map.put("collection_count", jedis.hget(newOrderName + i, "collection_count"));
                map.put("create_date", jedis.hget(newOrderName + i, "create_date"));
                map.put("news_type", jedis.hget(newOrderName + i, "news_type"));
                map.put("writer", jedis.hget(newOrderName + i, "writer"));
                map.put("url", jedis.hget(newOrderName + i, "url"));
                listOld.add(i, map);
                jedis.hset(Constant.NEWID_TO_ORDERID, jedis.hget(nowOrderName + indexs[i], "id"), i + "");
                jedis.del(nowOrderName + indexs[i]);
            }

            BigMap.newsOldList = listOld;

            Integer newsOrderTodayCount = Integer.parseInt(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

            List<Map<String, String>> listToday = new ArrayList<>();

            for (int i = 0; i < newsOrderTodayCount; i++) {
                Map<String, String> map = new HashedMap();
                map.put("news_id", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "id"));
                map.put("title", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "title"));
                map.put("image_url", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "image_url"));
                map.put("read_count", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "read_count"));
                map.put("comment_count", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "comment_count"));
                map.put("collection_count", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "collection_count"));
                map.put("create_date", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "create_date"));
                map.put("news_type", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "news_type"));
                map.put("writer", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "writer"));
                map.put("url", jedis.hget(Constant.NEWS_ORDER_TODAY_ + i, "url"));
                listToday.add(map);
            }

            BigMap.newsTodayList = listToday;

            getLogger().info("::::::::::::::::::::::::length::::::::::" + BigMap.newsTodayList.size());

            jedis.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NOW_NAME, newOrderName);

            jedis.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NEW_NAME, nowOrderName);

            BigMap.userNewsCount = new HashedMap();

            FlowFile write = session.write(flowFile, out -> {
                out.write("SUCCESS".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("新闻排序", e);
            FlowFile write = session.write(flowFile, out -> {
                out.write("FAILURE".getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_FAILURE);
        }
    }

}
