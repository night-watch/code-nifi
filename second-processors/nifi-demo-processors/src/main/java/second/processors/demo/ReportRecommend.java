package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.Constant;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import redis.clients.jedis.Jedis;
import second.serviceDemo.JedisSentinelPoolService;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/24.
 */
public class ReportRecommend extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_POOL = new PropertyDescriptor.Builder()
            .name("redis connection pool").description("redis连接池")
            .identifiesControllerService(JedisSentinelPoolService.class)
            .required(true).build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("type").description("用来处理那种类型的接口")
            .required(true).allowableValues("医院医生药品", "场馆教练课程", "食谱推荐", "所有食谱")
            .build();

    public static final Relationship MY_RELATIONSHIP_SUCCESS = new Relationship.Builder()
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

    private long time;

    private Map<String, List<String>> doctorIDs;

    private Map<String, List<String>> hospitalIDs;

    private Map<String, List<String>> drugIDs;

    private Map<String, List<Map<String, String>>> courseIDS;

    private Map<String, List<Map<String, String>>> venueINFOs;

    private Map<String, List<Map<String, String>>> coachINFOs;

    private Map<String, List<String>> recipesIDs;

    private Map<String, String> cityNameToCode;

//    private Set<String> cities;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_POOL);
        descriptors.add(TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP_SUCCESS);
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
        infos();
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
        try {

            String type = context.getProperty(TYPE).getValue();
            if ((new Date().getTime() - time) > 1000 * 60 *60) {
                infos();
                time = new Date().getTime();
            }

            JSONObject jsonObject = JSONObject.fromObject(content);
            String user_id = jsonObject.getString("appUserId");
            String report_id = jsonObject.getString("appReportId");
            String position = jsonObject.getString("position");

            position = cityNameToCode.get(position);

            if (position == null) {
                position = jedis.hget("user-" + user_id, "city_id");
            }

            String gender = "0".equals(jsonObject.getString("gender")) ? "男" : "女";
            JSONArray data = jsonObject.getJSONArray("data");
//        "医院医生药品", "场馆教练课程", "食谱推荐"
            JSONObject jsonReturn = null;
            switch (type) {
                case "医院医生药品" :
                    jsonReturn = medical(data, position);
                    break;
                case "场馆教练课程" :
                    jsonReturn = fitness(data, position, gender);
                    break;
                case "食谱推荐" :
                    jsonReturn = recipes(data);
                    break;
                case "所有食谱" :
                    jsonReturn = allRecipes(data);
                    break;
                default:
                    throw new RuntimeException("type属性值出错，不是想要的值");
            }
            jsonReturn.put("user_id", user_id);
            jsonReturn.put("report_id", report_id);
            jsonReturn.put("time", new Date().getTime());
            jsonReturn.put("code", "00000000");
            jsonReturn.put("message", "success");
            final JSONObject jsonFinal = jsonReturn;
            FlowFile write = session.write(flowFile, out -> {
                out.write(jsonFinal.toString().getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_SUCCESS);
        } catch (Exception e) {
            FlowFile write = session.write(flowFile, out -> {
                out.write(content.getBytes());
            });
            session.transfer(write, MY_RELATIONSHIP_FAILURE);
            getLogger().error(content, e);
        }
    }

    private void infos() {

        doctorIDs = new HashedMap();

        hospitalIDs = new HashedMap();

        drugIDs = new HashedMap();

        courseIDS = new HashedMap();

        venueINFOs = new HashedMap();

        coachINFOs = new HashedMap();

        recipesIDs = new HashedMap();

        cityNameToCode = jedis.hgetAll(Constant.CITY_NAME_CODE);

//        cities = jedisCluster.smembers("cities");

        Set<String> doctorClassifications = jedis.smembers(Constant.DOCTOR_CLASSIFICATIONS);
        for (String doctorClassification : doctorClassifications) {
            doctorIDs.put(doctorClassification, jedis.lrange(doctorClassification, 0, -1));
        }

        Set<String> hospitalClassifications = jedis.smembers(Constant.HOSPITAL_CLASSIFICATIONS);
        for (String hospitalClassification : hospitalClassifications) {
            hospitalIDs.put(hospitalClassification, jedis.lrange(hospitalClassification, 0, -1));
        }

        Set<String> drugClassifications = jedis.smembers(Constant.DRUG_CLASSIFICATIONS);
        for (String drugClassification : drugClassifications) {
            drugIDs.put(drugClassification, jedis.lrange(drugClassification, 0, -1));
        }

        Set<String> recipeClassifications = jedis.smembers(Constant.RECIPE_CLASSIFICATIONS);
        for (String recipeClassification : recipeClassifications) {
            recipesIDs.put(recipeClassification, jedis.lrange(recipeClassification, 0, -1));
        }

        Set<String> courseClassifications = jedis.smembers(Constant.COURSE_CLASSIFICATIONS);
        for (String courseClassification : courseClassifications) {
            List<Map<String, String>> list = new ArrayList<>();
            Set<String> courseIDes = jedis.smembers(courseClassification);
            for (String courseID : courseIDes) {
                list.add(jedis.hgetAll("courseID-" + courseID));
            }
            courseIDS.put(courseClassification, list);
        }

        Set<String> venueClassifications = jedis.smembers(Constant.VENUE_CLASSIFICATIONS);
        for (String venueClassification : venueClassifications) {
            List<Map<String, String>> list = new ArrayList<>();
            Set<String> venueIDes = jedis.smembers(venueClassification);
            for (String venueID : venueIDes) {
                list.add(jedis.hgetAll("venueID-" + venueID));
            }
            venueINFOs.put(venueClassification, list);
        }

        Set<String> coachClassifications = jedis.smembers(Constant.COACH_CLASSIFICATIONS);
        for (String coachClassification : coachClassifications) {
            List<Map<String, String>> list = new ArrayList<>();
            Set<String> coachIDes = jedis.smembers(coachClassification);
            for (String coachID : coachIDes) {
                list.add(jedis.hgetAll("coachID-" + coachID));
            }
            coachINFOs.put(coachClassification, list);
        }

    }

    private JSONObject recipes(JSONArray data) {
        if (data == null) {
            throw new RuntimeException("data数据为空");
        }
        List<Integer> breakfast = new LinkedList<>();
        List<Integer> lunch = new LinkedList<>();
        List<Integer> dinner = new LinkedList<>();
        List<Integer> addmeal = new LinkedList<>();
        for (int i = 0; i < data.size(); i++) {
            JSONObject index = data.getJSONObject(i);
            String symptoms = index.getString(Constant.SYMPTOMS);
            if (symptoms == null || symptoms.length() == 0) {
                continue;
            }
            breakfast = dealList(symptoms, 1, breakfast);
            lunch = dealList(symptoms, 2, lunch);
            dinner = dealList(symptoms, 3, dinner);
            addmeal = dealList(symptoms, 4, addmeal);
        }
        if (breakfast.size() == 0) {
            breakfast = noDisease("1");
        }
        if (lunch.size() == 0) {
            lunch = noDisease("2");
        }
        if (dinner.size() == 0) {
            dinner = noDisease("3");
        }
        if (addmeal.size() == 0) {
            addmeal = noDisease("4");
        }
        JSONObject dataReturn = new JSONObject();
        dataReturn.put("Monday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Tuesday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Wednesday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Thursday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Friday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Saturday", day(breakfast, lunch, dinner, addmeal));
        dataReturn.put("Sunday", day(breakfast, lunch, dinner, addmeal));
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(dataReturn);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", jsonArray);
        return jsonObject;
    }

    private List<Integer> noDisease(String type) {
        List<Integer> list = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : recipesIDs.entrySet()) {

            if (!entry.getKey().startsWith(type + "-")) {
                continue;
            }
            List<String> value = entry.getValue();
            for (int i = 0; i < value.size(); i++) {
                list.add(Integer.parseInt(value.get(i)));
            }
        }
        return list;
    }

    private JSONObject allRecipes(JSONArray data) {
        if (data == null) {
            throw new RuntimeException("data数据为空");
        }
        List<Integer> breakfast = new LinkedList<>();
        List<Integer> lunch = new LinkedList<>();
        List<Integer> dinner = new LinkedList<>();
        List<Integer> addmeal = new LinkedList<>();
        for (int i = 0; i < data.size(); i++) {
            JSONObject index = data.getJSONObject(i);
            String symptoms = index.getString(Constant.SYMPTOMS);
            if (symptoms == null) {
                continue;
            }
            breakfast = dealList(symptoms, 1, breakfast);
            lunch = dealList(symptoms, 2, lunch);
            dinner = dealList(symptoms, 3, dinner);
            addmeal = dealList(symptoms, 4, addmeal);
        }
        if (breakfast.size() == 0) {
            breakfast = noDisease("1");
        }
        if (lunch.size() == 0) {
            lunch = noDisease("2");
        }
        if (dinner.size() == 0) {
            dinner = noDisease("3");
        }
        if (addmeal.size() == 0) {
            addmeal = noDisease("4");
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("breakfast", breakfast);
        jsonObject.put("lunch", lunch);
        jsonObject.put("dinner", dinner);
        jsonObject.put("addmeal", addmeal);
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(jsonObject);
        JSONObject jo = new JSONObject();
        jo.put("data", jsonArray);
        return jo;
    }

    private JSONObject day(List<Integer> breakfast, List<Integer> lunch, List<Integer> dinner, List<Integer> addmeal) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("breakfast", deal(breakfast));
        jsonObject.put("lunch", deal(lunch));
        jsonObject.put("dinner", deal(dinner));
        jsonObject.put("addmeal", deal(addmeal));
        return jsonObject;
    }

    private Set<Integer> deal(List<Integer> list) {
        if (list.size() == 0) {
            return null;
        }

        int len = list.size() > 3 ? 3 : list.size();

        Set<Integer> ids = new HashSet<>();
        while (true) {
            int index = (int) (Math.random() * list.size());
            ids.add(list.get(index));
            if (ids.size() >= len) {
                break;
            }
        }
        return ids;
    }

    private List<Integer> dealList(String symptoms, Integer type, List<Integer> forReturn) {
        List<String> list = recipesIDs.get(type + "-" + symptoms + "-recipes");
        if (list == null || list.size() == 0) {
            return forReturn;
        }
        for (int i = 0; i < list.size(); i++) {
            int id = Integer.parseInt(list.get(i));
            if (!forReturn.contains(id)) {
                forReturn.add(id);
            }
        }
        return forReturn;
    }

    private JSONObject fitness(JSONArray data, String position, String gender) {
        List<String> courseIds = new LinkedList<>();
        List<Map<String, String>> venueInfos = new LinkedList<>();
        List<Map<String, String>> coachInfos = new LinkedList<>();
        for (int i = 0; i < data.size(); i++) {
            JSONObject index = data.getJSONObject(i);
            String symptoms = index.getString(Constant.SYMPTOMS);
            List<Map<String, String>> courseIdsList = courseIDS.get(symptoms + "-course");
            if (courseIdsList != null && courseIdsList.size() != 0) {
                for (int j = 0; j < courseIdsList.size(); j++) {
                    Map<String, String> map = courseIdsList.get(j);
                    if (gender.equals(map.get("gender")) || "通用".equals(map.get("gender"))) {
                        String id = map.get("id");
                        if (!courseIds.contains(id)) {
                            courseIds.add(id);
                        }
                    }
                }
            }

            List<Map<String, String>> courseIdsListGeneral = courseIDS.get("通用-course");
            if (courseIdsListGeneral != null && courseIdsListGeneral.size() != 0) {
                for (int j = 0; j < courseIdsListGeneral.size(); j++) {
                    Map<String, String> map = courseIdsListGeneral.get(j);
                    if (gender.equals(map.get("gender")) || "通用".equals(map.get("gender"))) {
                        String id = map.get("id");
                        if (!courseIds.contains(id)) {
                            courseIds.add(id);
                        }
                    }

                }
            }

            if (position == null || position.length() == 0) {
                continue;
            }

            List<Map<String, String>> venueInfosList = venueINFOs.get(position + "-" + symptoms + "-venue");
            venueInfos = add(venueInfosList, venueInfos);

            List<Map<String, String>> venueInfosListGeneral = venueINFOs.get(position + "-通用-venue");
            venueInfos = add(venueInfosListGeneral, venueInfos);

            List<Map<String, String>> coachInfosList = coachINFOs.get(position + "-" + symptoms + "-coach");
            coachInfos = add(coachInfosList, coachInfos);

            List<Map<String, String>> coachInfosListGeneral = coachINFOs.get(position + "-通用-coach");
            coachInfos = add(coachInfosListGeneral, coachInfos);

        }
        List<String> courseReturn = returnRandom(courseIds, 2);
        List<Map<String, String>> venueReturn = returnRandom(venueInfos, 2);
        List<Map<String, String>> coachReturn = returnRandom(coachInfos, 2);
        JSONObject jsonReturn = new JSONObject();
        jsonReturn.put("course_ids", courseReturn);
        jsonReturn.put("venues", venueReturn);
        jsonReturn.put("coaches", coachReturn);
        return jsonReturn;
    }



    private JSONObject medical(JSONArray data, String position) {
        List<String> hospitalIds = new LinkedList<>();
        List<String> doctorIds = new LinkedList<>();
        List<String> drugIds = new LinkedList<>();
        for (int i = 0; i < data.size(); i++) {
            JSONObject index = data.getJSONObject(i);
            String symptoms = index.getString(Constant.SYMPTOMS);
            if (symptoms == null || symptoms.length() == 0) {
                continue;
            }
            List<String> drugIDsList = drugIDs.get(symptoms + "-drug");
            drugIds = add(drugIDsList, drugIds);

            if (position == null || position.length() == 0) {
                continue;
            }

            List<String> doctorIDsList = doctorIDs.get(position + "-" + symptoms + "-doctor");
            doctorIds = add(doctorIDsList, doctorIds);

            List<String> hospitalIDsList = hospitalIDs.get(position + "-" + symptoms + "-hospital");
            hospitalIds = add(hospitalIDsList, hospitalIds);

        }
        List<String> drugIDReturn = returnRandom(drugIds, 2);
        List<String> hospitalIDReturn = returnRandom(hospitalIds, 3);
        List<String> doctorIDReturn = returnRandom(doctorIds, 3);
        JSONObject jsonReturn = new JSONObject();
        jsonReturn.put("hospital_ids", hospitalIDReturn);
        jsonReturn.put("doctor_ids", doctorIDReturn);
        jsonReturn.put("drug_ids", drugIDReturn);
        return jsonReturn;
    }

    private <T> List<T> add(List<T> list, List<T> forReturn) {
        if (list == null || list.size() == 0) {
            return forReturn;
        }
        for (int j = 0; j < list.size(); j++) {
            T id = list.get(j);
            if (!forReturn.contains(id)) {
                forReturn.add(id);
            }
        }
        return forReturn;
    }


    private <T> List<T> returnRandom(List<T> list, int length) {
        List<T> listReturn = new ArrayList<>();
        int len = list.size();
        for (int i = 0; i < length && i < len; i++) {
            int index = (int) (Math.random() * list.size());
            listReturn.add(list.remove(index));
        }
        return listReturn;
    }

}