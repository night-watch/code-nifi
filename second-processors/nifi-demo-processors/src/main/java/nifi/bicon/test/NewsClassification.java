package nifi.bicon.test;

import net.sf.json.JSONObject;
import nifi.bicon.util.JedisClusterUtil;
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
 * Created by NightWatch on 2018/1/14.
 * 同步新闻分类信息
 */
public class NewsClassification extends AbstractProcessor {

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

    private String host_name;

    private String portss;

    private String pwd;

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
            String ports = context.getProperty(PORTS).getValue();

            String hostname = context.getProperty(HOST_NAME).getValue();

            String password = context.getProperty(PASSWORD).getValue();

            if (!(password.equals(pwd) && hostname.equals(host_name) && ports.equals(portss))) {

                jedisClusterUtil = new JedisClusterUtil(hostname, ports, password);
                jedisCluster = jedisClusterUtil.getJedisCluster();
                pwd = password;
                host_name = hostname;
                portss = ports;
            }

            String content = new String(buffer, StandardCharsets.UTF_8);

            JSONObject jsonObject = JSONObject.fromObject(content);

            String id = String.valueOf(jsonObject.get("id"));

            String name = String.valueOf(jsonObject.get("name"));

            jedisCluster.hset("classification", id, name);

            session.transfer(flowFile, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("新闻分类信息同步", e);
            session.transfer(flowFile, MY_RELATIONSHIP_FAILURE);
        }
    }

}
