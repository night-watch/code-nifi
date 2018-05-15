package nifi.bicon.test;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.JedisClusterUtil;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.JedisCluster;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/18.
 */
public class LocalNews extends AbstractProcessor {

    public static final PropertyDescriptor PORTS = new PropertyDescriptor.Builder()
            .name("ports").description("redis连接端口号，多个端口用','分隔")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HOST_NAME = new PropertyDescriptor.Builder()
            .name("host name").description("连接redis的IP或主机名")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password").description("连接redis的密码")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true).build();


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

    private JedisClusterUtil jedisClusterUtil;

    private JedisCluster jedisCluster;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PORTS);
        descriptors.add(HOST_NAME);
        descriptors.add(PASSWORD);
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
        String ports = context.getProperty(PORTS).getValue();
        String hostname = context.getProperty(HOST_NAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
        jedisClusterUtil = new JedisClusterUtil(hostname, ports, password);
        jedisCluster = jedisClusterUtil.getJedisCluster();
    }

    @OnUnscheduled
    public void  unscheduled() {
        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            String position = jsonObject.getString("position");
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

            JSONArray data = recommend(userId, count, position);
            JSONObject back = new JSONObject();
            back.put("response_code", "00000000");
            back.put("response_msgs", "成功");
            back.put("response_time", new Date().getTime());
            back.put("user_id", userId);
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
    private JSONArray recommend(String userId, int count, String position) throws IOException,ClassNotFoundException {
        //查询已经推荐的新闻id组合，格式为,1,2,3,4,    !前后都加,
        String recommended = jedisCluster.hget("recommended", userId);
        if (recommended == null) {
            recommended = ",";
        }
        JSONArray data = new JSONArray();


        int today = (int) (count*0.4);
        //如果当天新闻小于20条，则去掉已经推荐过的，有多少推荐多少
        Integer newsOrderTodayCount = Integer.parseInt(jedisCluster.get("news_order_today_count"));
//        Long newsTodayLen = jedisCluster.llen("sortNews_today");
        if (0 < newsOrderTodayCount && newsOrderTodayCount <= today) {

            for (int i = 0; i < newsOrderTodayCount; i++) {

                String tags = jedisCluster.hget("news_order_today_" + i, "tags");

                if (tags != null && tags.contains(position)) {
                    String id = jedisCluster.hget("news_order_today_" + i, "id");
                    if (!recommended.contains("," + id + ",")) {
                        data.add(jedisCluster.hgetAll("news_order_today_" + i));
                        recommended = recommended.concat(id + ",");
                    }
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


                String tags = jedisCluster.hget("news_order_today_" + index, "tags");

                if (tags != null && tags.contains(position)) {
                    String id = jedisCluster.hget("news_order_today_" + index, "id");
                    if (!recommended.contains("," + id + ",")) {
                        data.add(jedisCluster.hgetAll("news_order_today_" + index));
                        recommended = recommended.concat(id + ",");
                    }
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
        Map<String, String> oldNewsNow = jedisCluster.hgetAll("old_news_now");

        Integer length = Integer.valueOf(oldNewsNow.get("length"));

        String nowOrderName = oldNewsNow.get("now_order_name");

        int index = 0;
        while (true) {
            //如果所有三天内的新闻全部推荐完，也无需再推
            if (index == length) {
                break;
            }
            String tags = jedisCluster.hget(nowOrderName + index, "tags");

            if (tags != null && tags.contains(position)) {
                String id = jedisCluster.hget(nowOrderName + index, "id");
                if (!recommended.contains("," + id + ",")) {
                    data.add(jedisCluster.hgetAll(nowOrderName + index));
                    recommended = recommended.concat(id + ",");
                }
            }
            index ++;
            //如果所有新闻推满100个也无需再推
            if (data.size() == count) {
                break;
            }
        }
        //将已经推荐的字符串更新到redis中
        jedisCluster.hset("recommended", userId, recommended);
        return data;
    }
}
