package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.JedisCreater;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import second.serviceDemo.JedisSentinelPoolService;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by NightWatch on 2017/11/10.
 */



@Tags({"recipes", "index_recipes"})
@CapabilityDescription("通过star和userString从数据库中查询推荐食谱")
public class ForRecipes extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_POOL = new PropertyDescriptor.Builder()
            .name("redis connection pool").description("redis连接池")
            .identifiesControllerService(JedisSentinelPoolService.class)
            .required(true).build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, such as an invalid query or an integrity constraint violation")
            .build();

    private Date date;

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REDIS_POOL);
        return properties;
    }

    private List<Map<String, String>> list = new ArrayList<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        date = new Date();
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
            JedisSentinelPoolService pool = context.getProperty(REDIS_POOL).asControllerService(JedisSentinelPoolService.class);
            Boolean hour = (new Date().getTime() - date.getTime() >= 1000*60*60);
            if (hour || list == null || list.size() == 0) {
                forRecipes(pool);
                date = new Date();
            }
            JSONArray jsonArray = ((JSONObject)jsonObject.get("data")).getJSONArray("index_list");
            List<Integer> breakfast = new ArrayList<>();
            List<Integer> lunch = new ArrayList<>();
            List<Integer> dinner = new ArrayList<>();
            List<Integer> addmeal = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject each = (JSONObject)jsonArray.get(i);
                String status = String.valueOf(each.get("status"));
                if ("0".equals(status)) {
                    continue;
                }
                String indexCode = String.valueOf(each.get("index_code"));
                String[] indexCodes = indexCode.split("_");
                String code = indexCodes[0] + "_" + indexCodes[1] + "_" +indexCodes[2];
                for (int j = 0; j < list.size(); j++){
                    Map<String, String> map = list.get(j);
                    if (map.get("dictionary_code").equals(code) && map.get("status").equals(String.valueOf(status))) {
                        if (map.get("type").equals("1")) {
                            int suggestRecipe = Integer.parseInt(map.get("suggest_recipe"));
                            breakfast = add(breakfast, suggestRecipe);
                        } else if (map.get("type").equals("2")) {
                            int suggestRecipe = Integer.parseInt(map.get("suggest_recipe"));
                            lunch = add(lunch, suggestRecipe);

                        } else if (map.get("type").equals("3")) {
                            int suggestRecipe = Integer.parseInt(map.get("suggest_recipe"));
                            dinner = add(dinner, suggestRecipe);

                        } else if (map.get("type").equals("4")) {
                            int suggestRecipe = Integer.parseInt(map.get("suggest_recipe"));
                            addmeal = add(addmeal, suggestRecipe);
                        }
                    }
                }
            }
            if (breakfast.size() == 0 && lunch.size() == 0 && dinner.size() == 0 && addmeal.size() == 0) {
                for (int i = 0; i < list.size(); i++) {
                    Map<String, String> map = list.get(i);
                    String type = map.get("type");
                    int suggestRecipe = Integer.parseInt(map.get("suggest_recipe"));
                    switch (type) {
                        case "1" :
                            breakfast = add(breakfast, suggestRecipe);
                            break;
                        case "2" :
                            lunch = add(lunch, suggestRecipe);
                            break;
                        case "3" :
                            dinner = add(dinner, suggestRecipe);
                            break;
                        case "4" :
                            addmeal = add(addmeal, suggestRecipe);
                            break;
                    }
                }
            }
            JSONObject jsonObjectData = new JSONObject();
            jsonObjectData.put("Monday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Tuesday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Wednesday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Thursday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Friday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Saturday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            jsonObjectData.put("Sunday", everydayRecipes(breakfast, lunch, dinner, addmeal));
            JSONArray jsonArrayData = new JSONArray();
            jsonArrayData.add(jsonObjectData);
            JSONObject jsonObjectNext = new JSONObject();
            jsonObjectNext.put("breakfast", breakfast);
            jsonObjectNext.put("lunch", lunch);
            jsonObjectNext.put("dinner", dinner);
            jsonObjectNext.put("addmeal", addmeal);
            JSONArray jsonArrayNext = new JSONArray();
            jsonArrayNext.add(jsonObjectNext);
            JSONObject jsonObjectReturn = new JSONObject();
            jsonObjectReturn.put("code", "00000000");
            jsonObjectReturn.put("message", "success");
            jsonObjectReturn.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            jsonObjectReturn.put("user_id", jsonObject.get("user_id"));
            jsonObjectReturn.put("data", jsonArrayData);
            jsonObjectReturn.put("next", jsonArrayNext);
            FlowFile writeFlowFile = session.write(flowFile, out -> {
                out.write(jsonObjectReturn.toString().getBytes());
            });
            session.transfer(writeFlowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("推荐食谱" + jsonObject, e);
            JSONObject data = new JSONObject();
            jsonObject.put("data", data);
            jsonObject.put("code", "00000001");
            jsonObject.put("message", "failure");
            FlowFile writeFlowFile = session.write(flowFile, out -> {
                out.write(jsonObject.toString().getBytes());
            });
            session.transfer(writeFlowFile, REL_FAILURE);
        }
    }

    private List<Integer> add(List<Integer> list, Integer suggestRecipe) {
        if (!list.contains(suggestRecipe)) {
            list.add(suggestRecipe);
        }
        return list;
    }

    @OnUnscheduled
    public void unscheduled() {
        list = new ArrayList<>();
        date = new Date(new Date().getTime() - 1000*60*60);
    }

    private void forRecipes(JedisSentinelPoolService pool) throws Exception{

        Jedis jedis = pool.getJedis();
        Map<String, String> recipes = jedis.hgetAll("recipes");
        for (Map.Entry entry : recipes.entrySet()) {
            Map<String, String> map = new HashedMap();
            JSONObject jsonObject = JSONObject.fromObject(entry.getValue());
            String suggest_recipes = String.valueOf(jsonObject.get("suggest_recipe"));
            String type = String.valueOf(jsonObject.get("type"));
            String dictionary_code = String.valueOf(jsonObject.get("dictionary_code"));
            String status = String.valueOf(jsonObject.get("status"));
            map.put("suggest_recipe", suggest_recipes);
            map.put("type", type);
            map.put("dictionary_code", dictionary_code);
            map.put("status", status);
            list.add(map);
        }
        jedis.close();
    }

    private JSONObject everydayRecipes(List<Integer> breakfast, List<Integer> lunch, List<Integer> dinner, List<Integer> addmeal) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("breakfast", everyMeal(breakfast));
        jsonObject.put("lunch", everyMeal(lunch));
        jsonObject.put("dinner", everyMeal(dinner));
        jsonObject.put("addmeal", everyMeal(addmeal));
        return jsonObject;
    }

    private Integer[] everyMeal(List<Integer> meal) {
        Integer[] meals = null;
        if (meal.size() > 3) {
            meals = new Integer[3];
            outer : for (int i = 0; i < 3; i++) {
                Integer random = new Double(Math.random()*meal.size()).intValue();
                Integer index = meal.get(random);
                if (meals == null) {
                    meals[i] = meal.get(random);
                    continue outer;
                }
                for (int j = 0; j < meals.length; j++) {
                    if (index == meals[j]) {
                        i--;
                        continue outer;
                    }
                }
                meals[i] = meal.get(random);
            }
        } else if (meal.size() == 0 || meal == null) {
            meals = new Integer[0];
        } else {
            meals = new Integer[meal.size()];
            for (int i = 0; i < meal.size(); i++) {
                meals[i] = meal.get(i);
            }
        }
        return meals;
    }
}