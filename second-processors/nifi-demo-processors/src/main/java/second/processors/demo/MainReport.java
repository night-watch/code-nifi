package second.processors.demo;

import net.sf.json.JSONArray;
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/24.
 */
public class MainReport extends AbstractProcessor {



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

//    private JedisClusterUtil jedisClusterUtil;
//
//    private JedisCluster jedisCluster;

    private Jedis jedis;

    private Map<String, Map<String, String>> targetInfos = new HashedMap();

    private List<String> diseases = new ArrayList<>();

    private long time;

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

        final List<String> diseases = new ArrayList<>();
        diseases.add("身高");
        diseases.add("体重");
        diseases.add("尿酸");
        diseases.add("收缩压");
        diseases.add("舒张压");
        diseases.add("空腹血糖");
        diseases.add("甘油三酯");
        diseases.add("总胆固醇");
        diseases.add("心率");
        this.diseases = Collections.unmodifiableList(diseases);

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
        Integer ports = context.getProperty(PORTS).asInteger();
        String hostname = context.getProperty(HOST_NAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
//        jedisClusterUtil = new JedisClusterUtil(hostname, ports, password);
//        jedisCluster = jedisClusterUtil.getJedisCluster();

        jedis = JedisCreater.create(hostname, ports, password);

        targetInfos();
        time = new Date().getTime();
    }

    /**
     * 在end时运行此方法，关闭redis连接，释放资源
     */
    @OnUnscheduled
    public void unscheduled() {

        jedis.close();

        /*try {
            jedisCluster.close();
        } catch (IOException e) {
            getLogger().error("更新新闻count中的Unscheduled异常", e);
        }*/

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



        try {
            JSONObject jsonObject = JSONObject.fromObject(content);
            if ((new Date().getTime() - time) > 1000 * 60 * 60) {
                targetInfos();
                time = new Date().getTime();
            }
            JSONArray data = jsonObject.getJSONArray("data");
            JSONArray dataReturn = new JSONArray();
            double height = 0;
            double weight = 0;
            String symptoms = ",";
            for (int i = 0; i < data.size(); i++) {
                JSONObject index = data.getJSONObject(i);
                String item = index.getString("item");
                String symptom = "";
                if ("肝".equals(item)) {
                    String result = index.getString("resultChar");

                    if (result.contains("脂肪肝")) {
                        symptom = "脂肪肝";
                    }

                    index.put(Constant.SYMPTOMS, symptom);
                    dataReturn.add(index);
                    continue;

                }
                if (!diseases.contains(item)) {
                    continue;
                }
                double result = index.getDouble("result");
                if ("身高".equals(item)) {
                    height = result;
                    continue;
                }
                if ("体重".equals(item)) {
                    weight = result;
                    continue;
                }
                Map<String, String> targetInfo = targetInfos.get(item);
                double upper = Double.parseDouble(targetInfo.get("upper"));
                double lower = Double.parseDouble(targetInfo.get("lower"));

                if (upper < result) {
                    String upperDisease = targetInfo.get("upper_disease");
                    if (upperDisease != null && !symptoms.contains("," + upperDisease + ",")) {

                        symptom = upperDisease;
//                        index.put(Constant.SYMPTOMS, upperDisease);
                        symptoms = symptoms + upperDisease + ",";
                    }
                } else if (lower > result) {
                    String lowerDisease = targetInfo.get("lower_disease");
                    if ((lowerDisease != null || lowerDisease.length() == 0) && !symptoms.contains("," + lowerDisease + ",")) {

                        symptom = lowerDisease;

//                        index.put(Constant.SYMPTOMS, lowerDisease);
                        symptoms = symptoms + lowerDisease + ",";
                    }
                }
                index.put(Constant.SYMPTOMS, symptom);
                dataReturn.add(index);
            }

            if (weight != 0 && height != 0) {
                JSONObject jsonChild = new JSONObject();
                double BMI = (weight / (height * height)) * 10000;
                Map<String, String> targetInfo = targetInfos.get("BMI指数");
                double upper = Double.parseDouble(targetInfo.get("upper"));
                double lower = Double.parseDouble(targetInfo.get("lower"));
                jsonChild.put("result", BMI);
                jsonChild.put("item", "BMI");
                jsonChild.put("unit", "");
                if (upper < BMI) {
                    String upperDisease = targetInfo.get("upper_disease");
                    if (upperDisease != null || upperDisease.length() == 0) {
                        jsonChild.put(Constant.SYMPTOMS, upperDisease);
                    }
                } else if (lower > BMI) {
                    String lowerDisease = targetInfo.get("lower_disease");
                    if (lowerDisease != null || lowerDisease.length() == 0) {
                        jsonChild.put(Constant.SYMPTOMS, lowerDisease);
                    }
                } else {
                    jsonChild.put(Constant.SYMPTOMS, "");
                }
                dataReturn.add(jsonChild);
            }

            jsonObject.put("data", dataReturn);

            FlowFile write = session.write(flowFile, out -> {
                out.write(jsonObject.toString().getBytes());
            });

            session.transfer(write, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("主报告MainReport异常" + content, e);

            FlowFile write = session.write(flowFile, out -> {
                out.write(content.getBytes());
            });

            session.transfer(write, MY_RELATIONSHIP_FAILURE);
        }

    }

    private void targetInfos() {
        Set<String> mainProjectNames = jedis.smembers(Constant.MAIN_PROJECT_NAMES);
        for (String projectName : mainProjectNames) {
            Map<String, String> info = jedis.hgetAll(projectName + "-info");
            targetInfos.put(projectName, info);
        }
    }

}
