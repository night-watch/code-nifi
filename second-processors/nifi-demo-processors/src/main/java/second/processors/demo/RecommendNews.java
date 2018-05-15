package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.BigMap;
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
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/13.
 * 用户新闻推荐
 */
public class RecommendNews extends AbstractProcessor {

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
        String userId = null;
        try {
            String content = new String(buffer, StandardCharsets.UTF_8);
            JSONObject jsonObject = JSONObject.fromObject(content);
            userId = String.valueOf(jsonObject.get("user_id"));
            Object requestDataNum = jsonObject.get("request_data_num");
            int count = 0;
            if (requestDataNum == null) {
                count = 100;
            } else {
                int num = Integer.valueOf(String.valueOf(requestDataNum));
                if (num >= 300) {
                    count = 300;
                } else if (num <= 50) {
                    count = 50;
                } else {
                    count = num;
                }
            }

            JSONArray data = recommend(userId, count);
            JSONObject back = new JSONObject();
            back.put("response_code", "00000000");
            back.put("response_msgs", "成功");
            back.put("response_time", new Date().getTime());
            back.put("user_id", userId);
            back.put("type", "1");
            back.put("data", data);
            FlowFile writeFlowfile = session.write(flowFile, out -> {
                out.write(back.toString().getBytes());
            });
            session.transfer(writeFlowfile, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            JSONObject back = failure(userId);
            FlowFile writeFlowfile = session.write(flowFile, out -> {
                out.write(back.toString().getBytes());
            });
            session.transfer(writeFlowfile, MY_RELATIONSHIP_FAILURE);
            getLogger().error("新闻推荐", e);
        }
    }

    /**
     * onTrigger方法捕获中执行的方法
     * @param userId
     * @return 返回失败的JSONObject
     */
    private JSONObject failure(String userId) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("response_code", "00000001");
        jsonObject.put("response_msgs", "失败");
        jsonObject.put("response_time", new Date().getTime());
        jsonObject.put("user_id", userId);
        return jsonObject;
    }

    /**
     * 根据userID去推荐新闻，并排除已经推荐的新闻。当天的新闻随机推荐，前三天的新闻按照排序推荐
     * @param userId 要推荐的用户ID
     * @return 所有新闻详细信息的json数组
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private JSONArray recommend(String userId, int count) throws IOException,ClassNotFoundException {

        //查询已经推荐的新闻id组合，格式为,1,2,3,4,    !前后都加,
        String recommended = jedis.hget(Constant.RECOMMENDED, userId);
        if (recommended == null || recommended.length() == 0) {
            recommended = ",";
        }


        JSONArray data = new JSONArray();

        int today = (int) (count*0.4);
        //如果当天新闻小于20条，则去掉已经推荐过的，有多少推荐多少
        Integer newsOrderTodayCount = BigMap.newsTodayList.size();
        if (0 < newsOrderTodayCount && newsOrderTodayCount <= today) {
            for (int i = 0; i < newsOrderTodayCount; i++) {
                String id = BigMap.newsTodayList.get(i).get("news_id");
                if (!recommended.contains("," + id + ",")) {
                    data.add(BigMap.newsTodayList.get(i));
                    recommended = recommended.concat(id + ",");
                }
            }
        //如果大于20条，则随机推荐20条没有推荐过的
        } else if (newsOrderTodayCount > today) {
            //新建一个LinkedList存放所有可能的下标，每当有已经选过的下标，则移除，防止又一次随机到，降低效率
            LinkedList<Integer> indexList = new LinkedList<>();
            for (int i = 0; i < newsOrderTodayCount; i ++) {
                indexList.add(i);
            }

            for (int i = 0; i < today; i ++) {
                int index = (int)(Math.random() * newsOrderTodayCount);

                Integer integer = indexList.get(index);

                String id = BigMap.newsTodayList.get(integer).get("news_id");

                if (!recommended.contains("," + id + ",")) {
                    data.add(BigMap.newsTodayList.get(integer));
                    recommended = recommended.concat(id + ",");
                } else {
                    i --;
                }
                indexList.remove(index);
                newsOrderTodayCount --;
                //如果所有的下标已经随机完，则无需再循环，即使没有到20个也终止循环
                if (indexList.size() == 0) {
                    break;
                }
            }
        }

        //前三天的新闻按照排序来推荐
        Integer length = BigMap.newsOldList.size();

        int index = 0;

        Integer newsCount = BigMap.userNewsCount.get(userId);

        if (newsCount != null) {
            index = newsCount;
        }

        while (true) {
            //如果所有三天内的新闻全部推荐完，也无需再推
            if (index == length) {
                break;
            }
            String id = BigMap.newsOldList.get(index).get("news_id");

            if (!recommended.contains("," + id + ",")) {
                data.add(BigMap.newsOldList.get(index));
                recommended = recommended.concat(id + ",");
            }
            index ++;
            //如果所有新闻推满100个也无需再推
            if (data.size() == count) {
                break;
            }
        }
        BigMap.userNewsCount.put(userId, index);
        //将已经推荐的字符串更新到redis中
        jedis.hset(Constant.RECOMMENDED, userId, recommended);
        return data;
    }

}
