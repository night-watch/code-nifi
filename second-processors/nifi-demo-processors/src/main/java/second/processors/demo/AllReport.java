package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.Constant;
import nifi.bicon.util.JedisClusterUtil;
import nifi.bicon.util.JedisCreater;
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/29.
 */
public class AllReport extends AbstractProcessor {

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

    private Map<String, String> allLimit;

    private long time;


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
        limit();
        time = new Date().getTime();
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

        String content = new String(buffer, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.fromObject(content);
        try {
            if ((new Date().getTime() - time) > 1000 * 60 * 60) {
                limit();
                time = new Date().getTime();
            }
            JSONArray data = jsonObject.getJSONArray("data");
            String userId = jsonObject.getString("appUserId");

            String mainReportId = jsonObject.getString("appReportId");
            JSONArray returnData = new JSONArray();
            for (int i = 0; i < data.size(); i++) {
                JSONObject index = data.getJSONObject(i);
                String result = index.getString("result");
                if (!isNum(result)) {
                    continue;
                }
                double value = Double.parseDouble(result);
                String item = index.getString("item");
                String s = allLimit.get(item);
                if (s == null || s.length() == 0) {
                    continue;
                }
                String[] split = s.split(",");
                JSONObject returnIndex = new JSONObject();

                String s0 = "";
                String s1 = "";

                if (split.length == 1) {
                    s0 = split[0];
                } else if (split.length != 0) {
                    s0 = split[0];
                    s1 = split[1];
                }
                if (s0.length() == 0) {
                    s0 = value - 1 + "";
                }
                if (s1.length() == 0) {
                    s1 = value + 1 + "";
                }

                int status;


                if (value <= Double.parseDouble(s0)) {

                    status = -1;
                } else if (value > Double.parseDouble(s1)) {

                    status = 1;

                } else {
                    continue;
                }
                returnIndex.put("indexName", item);
                returnIndex.put("indexValue", result);
                returnIndex.put("indexUnit", index.getString("unit"));
                returnIndex.put("status", status);
                returnIndex.put("highContent", "您的" + item + "偏高!");
                returnIndex.put("lowContent", "您的" + item + "偏低!");
                returnData.add(returnIndex);
            }
            JSONObject returnJson = new JSONObject();
            returnJson.put("code", "00000000");
            returnJson.put("message", "success");
            returnJson.put("userId", userId);
            returnJson.put("reportId", mainReportId);
            returnJson.put("data", returnData);
            returnJson.put("tip", jsonObject.getString("conclusion"));
            returnJson.put("time", new Date().getTime());
            FlowFile write = session.write(flowFile, out -> {
                out.write(returnJson.toString().getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("AllReport出错", e);
            jsonObject.put("data", null);
            jsonObject.put("code", "00000001");
            jsonObject.put("message", "failure");
            FlowFile write = session.write(flowFile, out -> {
                out.write(jsonObject.toString().getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_FAILURE);
        }
    }

    private void limit() {
        allLimit = jedis.hgetAll(Constant.ALL_LIMIT);
    }

    private boolean isNum(String str) {

        if (str == null || str.length() == 0) {
            return false;
        }

        boolean flag = true;
        for (int i = 0; i < str.length(); i++) {
            char chr = str.charAt(i);
            if ((chr < 48 || chr > 57) && !(chr == 46 && i != 0 && i != (str.length() - 1) && flag)) {
                if (i == 0 && chr == 45 && str.length() >= 2) {
                    continue;
                }
                return false;
            }
        }
        return true;
    }
}
