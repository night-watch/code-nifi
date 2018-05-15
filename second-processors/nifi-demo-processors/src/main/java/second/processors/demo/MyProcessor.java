/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.Constant;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="my.property", description="读取当前处理器的配置信息，并写入WriteAttribute中")})
public class MyProcessor extends AbstractProcessor {

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


    // 自定义属性值列表
    private List<PropertyDescriptor> descriptors;

    private Map<String, List<Map<String, String>>> formulas;

    // 关系集合
    private Set<Relationship> relationships;

    //以录入体征code为键，存放high_content,low_content,priority,standard_value,small_value,big_value以及标准体征code
    private final Map<String, Map<String, String>> infos = new LinkedHashMap<>();

    //用来存放录入体征code和标准体征code的映射关系
    private Map<String, String> relation = new HashedMap();

    private ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");

    private Jedis jedis;

    private Date date;

    private DecimalFormat decimalFormat = new DecimalFormat("######0");

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_POOL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        date = new Date();

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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

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

            Boolean hour = (new Date().getTime() - date.getTime() >= 1000*60*60);

            if (hour || formulas == null || formulas.size() == 0) {
                forMap();
                date = new Date();
            }
            final JSONObject jsonObjectBack = forReturn(jsonObject);
            FlowFile writeFlowFile = session.write(flowFile, out -> {
                out.write(jsonObjectBack.toString().getBytes());
            });
            session.transfer(writeFlowFile, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("推荐星级" + content, e);
            JSONObject data = new JSONObject();
            data.put("data", null);
            data.put("code", "00000001");
            data.put("message", "failure");
            FlowFile writeFlowFile = session.write(flowFile, out -> {
                out.write(data.toString().getBytes());
            });
            session.transfer(writeFlowFile, MY_RELATIONSHIP_FAILURE);
        }
    }

    @OnUnscheduled
    public void unscheduled() {
        date = new Date(new Date().getTime() - 1000*60*60);
        jedis.close();
    }

    private JSONObject forReturn(JSONObject jsonObject) throws ScriptException {
        String tip = null;
        Integer comparePriority = 4;
        String userId = String.valueOf(jsonObject.get("user_id"));
        jedis.sadd(Constant.USER_TARGET_SET, userId);
        JSONArray jsonArray = jsonObject.getJSONArray("data");
        JSONArray jsonArrayBack = new JSONArray();
        Double goals = 0.0;
        for (int i = 0; i < jsonArray.size(); i++){
            JSONObject jsonObjectChild = (JSONObject) jsonArray.get(i);
            String indexCode = jsonObjectChild.get("index_code").toString();
            Double smallValue = Double.parseDouble(infos.get(indexCode).get("small_value"));
            Double bigValue = Double.parseDouble(infos.get(indexCode).get("big_value"));
            Integer priority = Integer.parseInt(infos.get(indexCode).get("priority"));
            String highContent = infos.get(indexCode).get("high_content");
            String lowContent = infos.get(indexCode).get("low_content");
            Double indexValue = Double.parseDouble((String)jsonObjectChild.get("index_value"));
            String indexCodeBZZB = null;
            for (Map.Entry<String, String> entry : relation.entrySet()) {
                if (entry.getValue().equals(indexCode)) {
                    indexCodeBZZB = entry.getKey();
                    break;
                }
            }
            Double goal = 0.0;
            List<Map<String, String>> list = formulas.get(indexCodeBZZB);
            for (int j = 0; j < list.size(); j ++) {
                Map<String, String> map = list.get(j);

                Double indexLower = Double.parseDouble(map.get("index_lower"));

                Double indexUpper = Double.parseDouble(map.get("index_upper"));

                if (indexLower <= indexValue && indexUpper >= indexValue) {
                    String formula = map.get("goal_formula");
                    String formulaReplace = formula.replaceAll("x0", String.valueOf(indexValue));
                    goal = Double.parseDouble(String.valueOf(jse.eval(formulaReplace))) * Double.parseDouble(map.get("weight"));
                }
            }

            Integer status = 0;
            if (indexValue > bigValue) {
                status = 1;
            } else if (indexValue < smallValue) {
                status = -1;
            }
            jsonObjectChild.put("status", status);
            jsonObjectChild.put("high_content", highContent);
            jsonObjectChild.put("low_content", lowContent);
            jsonObjectChild.put("priority", priority);
            jsonObjectChild.put("exp_value", Integer.valueOf(decimalFormat.format(goal)));
            jsonObjectChild.put("time", new Date().getTime());
            String key = Constant.USER_LRTZ + userId;
            jedis.hset(key, indexCode, jsonObjectChild.toString());
        }
        Calendar timeZero = Calendar.getInstance();
        timeZero.set(Calendar.HOUR_OF_DAY, 0);
        timeZero.set(Calendar.SECOND, 0);
        timeZero.set(Calendar.MINUTE, 0);
        timeZero.set(Calendar.MILLISECOND, 0);
        long timeInMillis = timeZero.getTimeInMillis();
        Map<String, String> map = jedis.hgetAll(Constant.USER_LRTZ + userId);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String value = entry.getValue();

            JSONObject fromRedis = JSONObject.fromObject(value);

            int status = Integer.parseInt(fromRedis.getString("status"));
            long time = Long.parseLong(fromRedis.getString("time"));

            fromRedis.remove("time");
            if (timeInMillis > time) {
                continue;
            }
            int expValue = fromRedis.getInt("exp_value");
            goals += expValue;
            jsonArrayBack.add(fromRedis);
            int priority = Integer.parseInt(fromRedis.getString("priority"));
            if (status == 1) {
                if (priority < comparePriority) {
                    tip = fromRedis.getString("high_content");
                    comparePriority = priority;
                }
            } else if (status == -1) {
                if (priority < comparePriority) {
                    tip = fromRedis.getString("low_content");
                    comparePriority = priority;
                }
            }
        }

        int expValue = Integer.valueOf(decimalFormat.format(goals));

        int star = (expValue - 1) / 20 + 1;

        String level;
        if (star == 5) {
            level = "良好";
        } else if (star == 4 || star == 3 || star == 2 || star == 1) {
            level = "正常";
        } else {
            level = "异常";
        }
        JSONObject jsonObjectChildBack = new JSONObject();
        jsonObjectChildBack.put("star", star);
        jsonObjectChildBack.put("level", level);
        jsonObjectChildBack.put("index_list", jsonArrayBack);
        jsonObjectChildBack.put("tip", tip);
        jsonObjectChildBack.put("exp_value", expValue);
        jsonObjectChildBack.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        jsonObject.put("code", "000000000");
        jsonObject.put("message", "success");
        jsonObject.put("data", jsonObjectChildBack);
        return jsonObject;

    }

    private void forMap() throws IOException {
        relation = jedis.hgetAll("relation");

        for (Map.Entry<String, String> entry : relation.entrySet()) {
            String value = entry.getValue();
            Map<String, String> info = jedis.hgetAll(value);
            infos.put(value, info);
        }

        formulas = new HashedMap();
        Map<String, String> form = jedis.hgetAll("formulas");
        for (Map.Entry entry : form.entrySet()) {
            JSONObject formula = JSONObject.fromObject(String.valueOf(entry.getValue()));
            String dictionaryCode = String.valueOf(formula.get("dictionary_code"));
            String indexLower = String.valueOf(formula.get("index_lower"));
            String indexUpper = String.valueOf(formula.get("index_upper"));
            String goalFormula = String.valueOf(formula.get("formula"));
            String weight = String.valueOf(formula.get("weight"));
            Map<String, String> map = new HashedMap();
            map.put("index_lower", indexLower);
            map.put("index_upper", indexUpper);
            map.put("goal_formula", goalFormula);
            map.put("weight", weight);
            if (formulas.get(dictionaryCode) == null) {
                formulas.put(dictionaryCode, new ArrayList<>());
            }
            formulas.get(dictionaryCode).add(map);
        }
    }
}
