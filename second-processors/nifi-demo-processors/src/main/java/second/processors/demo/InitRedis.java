package second.processors.demo;

import net.sf.json.JSONObject;
import nifi.bicon.util.Constant;
import nifi.bicon.util.JedisCreater;
import org.apache.commons.collections.map.HashedMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.sql.*;
import java.util.*;

/**
 * Created by NightWatch on 2018/1/5.
 */
public class InitRedis {

    public static void main(String[] args) throws Exception{


        /*Set<HostAndPort> hostAndPortSet = new HashSet<>();
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17001));
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17002));
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17003));
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17004));
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17005));
        hostAndPortSet.add(new HostAndPort("10.1.24.216", 17006));

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxIdle(2);

        JedisCluster jedisCluster = new JedisCluster(hostAndPortSet, 5000, 5000, 3, "Kingleading", jedisPoolConfig);*/

//        Jedis jedis = JedisCreater.create("10.1.24.216", 27000, "Kingleading");

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(5000);
        config.setMaxIdle(256);
        config.setMaxWaitMillis(5000L);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setMinEvictableIdleTimeMillis(60000L);
        config.setTimeBetweenEvictionRunsMillis(3000L);
        config.setNumTestsPerEvictionRun(-1);

        Set<String> sentinels = new HashSet<>();

        sentinels.add("10.1.24.216:37000");

        System.out.println(sentinels);

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("mymaster", sentinels, config, "Kingleading");

        Jedis jedis = jedisSentinelPool.getResource();


        star(jedis);

        re(jedis);

        formula(jedis);

        gym(jedis);


        allProject(jedis);

        coach(jedis);
        course(jedis);

        city(jedis);

        recipesReport(jedis);

        projectName(jedis);

        doctor(jedis);

        hospital(jedis);

        userCity(jedis);

        drug(jedis);

//        newsInfoToRedis(jedisCluster);

        jedis.close();


    }


/*    private static void newsInfoToRedis(JedisCluster jedisCluster) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/news", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        String sql = "SELECT id, title, image_url, type, tags, src, writer, url, read_count, comment_count, collection_count, UNIX_TIMESTAMP(create_date), count, action_type_x FROM news_info";
        ResultSet resultSet = mySQLStatement.executeQuery(sql);

        while (resultSet.next()) {

            String id = resultSet.getString(1);
            String title = resultSet.getString(2);
            String image_url = resultSet.getString(3);
            String type = resultSet.getString(4);
            String tags = resultSet.getString(5);
            String src = resultSet.getString(6);
            String writer = resultSet.getString(7);
            String url = resultSet.getString(8);
            String read_count = resultSet.getString(9);
            String comment_count = resultSet.getString(10);
            String collection_count = resultSet.getString(11);
            long create_date = resultSet.getLong(12) * 1000;
            String all_count = resultSet.getString(13);
            String action_type_x = resultSet.getString(14);
            String newsType = "2";
            if (image_url.length() == 0) {
                newsType = "0";
            } else if (image_url.split(",").length < 3) {
                newsType = "1";
            }
            Map<String, String> newsInfo = new HashedMap();
            newsInfo.put("id", id);
            newsInfo.put("title", title);
            newsInfo.put("image_url", image_url);
            newsInfo.put("type", type);
            newsInfo.put("tags", tags);
            newsInfo.put("src", src);
            newsInfo.put("writer", writer);
            newsInfo.put("url", url);
            newsInfo.put("read_count", read_count);
            newsInfo.put("comment_count", comment_count);
            newsInfo.put("collection_count", collection_count);
            newsInfo.put("create_date", create_date + "");
            newsInfo.put("all_count", all_count);
            newsInfo.put("action_type_x", action_type_x);
            newsInfo.put("newsType", newsType);

            int x = Integer.parseInt(jedisCluster.get(Constant.NEWS_ORDER_TODAY_COUNT));

            Set<String> newsIDs = jedisCluster.hkeys(Constant.NEWID_TO_ORDERID);

            if (!newsIDs.contains(id)) {

                jedisCluster.hmset(Constant.NEWS_ORDER_TODAY_ + x, newsInfo);

                jedisCluster.hset(Constant.NEWID_TO_ORDERID, id, Constant.NEWS_ORDER_TODAY_ + x);

                jedisCluster.incrBy(Constant.NEWS_ORDER_TODAY_COUNT, 1);

            }

        }

    }*/


    private static void allProject(Jedis jedis) throws SQLException, ClassNotFoundException {

        String sql = "SELECT project_name, lower, upper FROM all_report_info";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/medical_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String projectName = resultSet.getString(1);
            String lower = resultSet.getString(2);
            String upper = resultSet.getString(3);

            if (lower == null) {

                lower = "";
            }

            if (upper == null) {
                upper = "";
            }


//            System.out.println(lower + "," + upper);

            jedis.hset(Constant.ALL_LIMIT, projectName, lower + "," + upper);

//            jedisCluster.hset("user-" + id, "city_id", cityId);

//            jedisCluster.hset(Constant.CITY_NAME_CODE, dictionaryName, dictionaryCode);
        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();
    }

    private static void userCity(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT id, city_id FROM user_info_synchro WHERE NOT city_id = ''";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String id = resultSet.getString(1);
            String cityId = resultSet.getString(2);

            System.out.println(id + ", \t" + cityId);

            jedis.hset("user-" + id, "city_id", cityId);

//            jedisCluster.hset(Constant.CITY_NAME_CODE, dictionaryName, dictionaryCode);
        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();
    }

    private static void city(Jedis jedis) throws ClassNotFoundException, SQLException {

        String sql = "SELECT dictionary_name, dictionary_code FROM dictionary WHERE TYPE = 0";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/base_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String dictionaryName = resultSet.getString(1);
            String dictionaryCode = resultSet.getString(2);
//            System.out.println(dictionaryCode + ", \t" + dictionaryName);

            jedis.hset(Constant.CITY_NAME_CODE, dictionaryName, dictionaryCode);
        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();
    }


    private static void course(Jedis jedis) throws ClassNotFoundException, SQLException {

        String sql = "SELECT disease, gender, course_id FROM disease_course_rel_tmp";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/fitness_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String disease = resultSet.getString(1);
            String gender = resultSet.getString(2);
            String courseId = resultSet.getString(3);



            jedis.sadd(Constant.COURSE_CLASSIFICATIONS, disease + "-course");
            jedis.sadd(disease + "-course", courseId);
            jedis.hset("courseID-" + courseId, "gender", gender);
            jedis.hset("courseID-" + courseId, "id", courseId);

        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();



    }


    private static void coach(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT b.pho, b.name, b.cch_desc, b.std_num, b.work_time, b.fbk_rate, b.cch_id, d.disease, g.addr, b.city FROM bas_cch b, disease_cch_rel_tmp d, bas_gym g WHERE b.cch_id = d.cch_id AND b.gym_id = g.gym_id";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/fitness_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String coachImage = resultSet.getString(1);
            String coachName = resultSet.getString(2);
            String coachIntroduce = resultSet.getString(3);
            String participationCount = resultSet.getString(4);
            String workTime = resultSet.getString(5);
            String applauseRate = resultSet.getString(6);
            String coachId = resultSet.getString(7);
            String disease = resultSet.getString(8);
            String venueAddress = resultSet.getString(9);
            String city = resultSet.getString(10);



            jedis.sadd(Constant.COACH_CLASSIFICATIONS, city + "-" + disease + "-coach");
            jedis.sadd(city + "-" + disease + "-coach", coachId);
            jedis.hset("coachID-" + coachId, "coach_image", coachImage);
            jedis.hset("coachID-" + coachId, "coach_name", coachName);
            jedis.hset("coachID-" + coachId, "coach_introduce", coachIntroduce);
            jedis.hset("coachID-" + coachId, "participation_count", participationCount);
            jedis.hset("coachID-" + coachId, "venue_address", venueAddress);
            jedis.hset("coachID-" + coachId, "work_time", workTime);
            jedis.hset("coachID-" + coachId, "applause_rate", applauseRate);


        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();



    }


    //场馆
    private static void gym(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT b.gym_name, b.per_psn_csm, b.addr, b.pto, b.gym_id, d.disease, b.city FROM bas_gym b, disease_gym_rel_tmp d WHERE b.gym_id = d.gym_id";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/fitness_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String GYM_NAME = resultSet.getString(1);
            String PER_PSN_CSM = resultSet.getString(2);
            String ADDR = resultSet.getString(3);
            String PHO = resultSet.getString(4);
            String GYM_ID = resultSet.getString(5);
            String disease = resultSet.getString(6);
            String city = resultSet.getString(7);

            jedis.sadd(Constant.VENUE_CLASSIFICATIONS, city + "-" + disease + "-venue");
            jedis.sadd(city + "-" + disease + "-venue", GYM_ID);
            jedis.hset("venueID-" + GYM_ID, "venue_name", GYM_NAME);
            jedis.hset("venueID-" + GYM_ID, "venue_price", PER_PSN_CSM);
            jedis.hset("venueID-" + GYM_ID, "venue_address", ADDR);
            jedis.hset("venueID-" + GYM_ID, "venue_image", PHO);

        }
        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();




    }




    private static void drug(Jedis jedis) throws ClassNotFoundException, SQLException {

        String[] dis = new String[]{"%肥胖%", "%消瘦%", "%糖尿病%", "%高血糖%", "%高血压%", "%高血脂%", "%胆固醇%", "%脂肪肝%"};

        String[] di = new String[]{"肥胖", "消瘦", "高血糖", "高血糖", "高血压", "高血脂", "高血脂", "脂肪肝"};

        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/medical_test", "root", "123456");

        String sql = "SELECT disease, drug_id FROM disease_drug_rel_tmp WHERE disease LIKE ?";
        PreparedStatement preparedStatement = mySQLConnection.prepareStatement(sql);


        for (int i = 0; i < dis.length; i++) {
            preparedStatement.setString(1, dis[i]);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {

                String id = resultSet.getString(2);

                jedis.sadd(Constant.DRUG_CLASSIFICATIONS, di[i] + "-drug");
                jedis.lpush(di[i] + "-drug", id);
            }
            resultSet.close();
        }
        preparedStatement.close();
        mySQLConnection.close();
    }

    private static void hospital(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT d.disease, d.hospital_id, h.city_id FROM disease_hospital_rel_tmp d, hospital h WHERE d.hospital_id = h.id";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/medical_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);

        while (resultSet.next()) {
            String symptoms = resultSet.getString(1);
            String id = resultSet.getString(2);
            String cityID = resultSet.getString(3);

//            System.out.println(symptoms + "," + id + "," + cityId);

            String classification = cityID + "-" + symptoms + "-hospital";

            jedis.sadd(Constant.HOSPITAL_CLASSIFICATIONS, classification);

            jedis.lpush(classification, id);
        }

        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();



    }

    private static void doctor(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT i.disease, i.doctor_id, o.city_id FROM disease_doctor_rel_tmp i, doctor o WHERE i.doctor_id = o.id";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/medical_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);

        while (resultSet.next()) {
            String symptoms = resultSet.getString(1);
            String id = resultSet.getString(2);
            String cityID = resultSet.getString(3);

            String classification = cityID + "-" + symptoms + "-doctor";

            jedis.sadd(Constant.DOCTOR_CLASSIFICATIONS, classification);

            jedis.lpush(classification, id);
        }

        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();
    }



    private static void projectName(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "SELECT project_name, lower, upper, lower_disease, upper_disease, lower_tips, upper_tips FROM main_project_info";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/medical_test", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);

        while (resultSet.next()) {
            String projectName = resultSet.getString(1);
            String lower = resultSet.getString(2);
            String upper = resultSet.getString(3);
            String lowerDisease = resultSet.getString(4);
            String upperDisease = resultSet.getString(5);

            jedis.sadd(Constant.MAIN_PROJECT_NAMES, projectName);

            jedis.hset(projectName + "-info", "lower", lower);
            jedis.hset(projectName + "-info", "upper", upper);
            jedis.hset(projectName + "-info", "lower_disease", lowerDisease);
            jedis.hset(projectName + "-info", "upper_disease", upperDisease);

        }

        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();

    }


    private static void recipesReport(Jedis jedis) throws ClassNotFoundException, SQLException {

        String sql = "select disease,type,recipes_id FROM disease_recipe_rel_tmp";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        ResultSet resultSet = mySQLStatement.executeQuery(sql);

        while (resultSet.next()) {
            String disease = resultSet.getString(1);
            String type = resultSet.getString(2);
            String recipes_id = resultSet.getString(3);
            String classification = type + "-" + disease + "-recipes";
            jedis.sadd(Constant.RECIPE_CLASSIFICATIONS, classification);
            jedis.lpush(classification, recipes_id);
        }


        resultSet.close();
        mySQLStatement.close();
        mySQLConnection.close();
    }

    private static void star(Jedis jedis) throws ClassNotFoundException, SQLException {
        Map<String, Map<String, String>> map = new LinkedHashMap<>();
        Map<String, String> relation = new HashedMap();
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health_data", "root", "123456");
        Statement mySQLStatement = mySQLConnection.createStatement();
        String sqlStandardData = "select dictionary_code from standard_data";
        ResultSet resultSetStandardData = mySQLStatement.executeQuery(sqlStandardData);
        while (resultSetStandardData.next()) {
            String dictionaryCode = resultSetStandardData.getString(1);
            relation.put(dictionaryCode, null);
        }
        resultSetStandardData.close();
        String sqlIndexTip = "select dictionary_code from index_tip";
        ResultSet resultSetIndexTip = mySQLStatement.executeQuery(sqlIndexTip);
        while (resultSetIndexTip.next()) {
            String dictionaryCode = resultSetIndexTip.getString(1);
            for (Map.Entry<String, String> entry : relation.entrySet()) {
                String key = entry.getKey();
                if (dictionaryCode.substring(10).equals(key.substring(8))) {
                    relation.replace(key, dictionaryCode);
                }
            }
        }
        System.out.println(1);
        String sql = "select dictionary_code,high_content,low_content,priority from index_tip where logic_delete = 0";
        ResultSet resultSet = mySQLStatement.executeQuery(sql);
        while (resultSet.next()) {
            String dictionary_code = resultSet.getString(1);
            String high_content = resultSet.getString(2);
            String low_content = resultSet.getString(3);
            Integer priority = resultSet.getInt(4);
            Map<String, String> value = new HashedMap();
            value.put("high_content", high_content);
            value.put("low_content", low_content);
            value.put("priority", priority.toString());
            map.put(dictionary_code, value);
        }
        String sql2 = "select dictionary_code,standard_value,small_value,big_value from standard_data where logic_delete = 0";
        resultSet = mySQLStatement.executeQuery(sql2);
        while (resultSet.next()) {
            String dictionary_code = relation.get(resultSet.getString(1));
            map.get(dictionary_code).put("dictionary_code", relation.get(resultSet.getString(1)));
            map.get(dictionary_code).put("standard_value", resultSet.getString(2));
            map.get(dictionary_code).put("small_value", resultSet.getString(3));
            map.get(dictionary_code).put("big_value", resultSet.getString(4));
        }

        System.out.println(2);
        for (Map.Entry<String, String> entry : relation.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            jedis.hset("relation", key, value);
        }

        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            String key = entry.getKey();
            Map<String, String> value = entry.getValue();

            for (Map.Entry<String, String> entry1 : value.entrySet()) {
                String key1 = entry1.getKey();
                String value1 = entry1.getValue();

                System.out.println("key" + key);
                System.out.println("key1" + key1);
                System.out.println("value" + value1);
                jedis.hset(key, key1, value1);
            }
        }
        resultSetIndexTip.close();
        mySQLStatement.close();
        mySQLConnection.close();
        System.out.println(3);
    }

    private static void formula(Jedis jedis) throws SQLException, ClassNotFoundException {
        String sql = "SELECT dictionary_code,index_lower,index_upper,formula,weight FROM goal_formula";

        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health_second", "root", "123456");
        Statement statement = mySQLConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        int num = 1;
        while (resultSet.next()) {
            String dictionary_code = resultSet.getString(1);
            String index_lower = resultSet.getString(2);
            String index_upper = resultSet.getString(3);
            String formula = resultSet.getString(4);
            String weight = resultSet.getString(5);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("dictionary_code", dictionary_code);
            jsonObject.put("index_lower", index_lower);
            jsonObject.put("index_upper", index_upper);
            jsonObject.put("formula", formula);
            jsonObject.put("weight", weight);
            jedis.hset("formulas", String.valueOf(num), jsonObject.toString());
            num ++;
        }
        resultSet.close();
        statement.close();
        mySQLConnection.close();
    }

    private static void recipes(Jedis jedis) throws SQLException, ClassNotFoundException {
        String sql = "select i.suggest_recipes,r.type,i.dictionary_code,i.status from index_recipes i,recipes r where i.suggest_recipes = r.id";
        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health", "root", "123456");
        Statement statement = mySQLConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        Integer i = 1;
        while (resultSet.next()) {
            String suggest_recipe = resultSet.getString(1);
            String type = resultSet.getString(2);
            String dictionary_code = resultSet.getString(3);
            String status = resultSet.getString(4);


            JSONObject jsonObject = new JSONObject();

            jsonObject.put("suggest_recipe", suggest_recipe);

            jsonObject.put("type", type);

            jsonObject.put("dictionary_code", dictionary_code);
            jsonObject.put("status", status);

            jedis.hset("recipes", String.valueOf(i), jsonObject.toString());
            i ++;
        }
        resultSet.close();
        statement.close();
        mySQLConnection.close();

    }


    public static void re(Jedis jedis) throws ClassNotFoundException, SQLException {
        String sql = "select dictionary_code, status, suggest_recipes from index_recipes where logic_delete = 0";

        Class.forName("com.mysql.jdbc.Driver");
        Connection mySQLConnection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/life_health", "root", "123456");
        Statement statement = mySQLConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        List<Map<String, String>> list = new ArrayList<>();
        while (resultSet.next()) {
            String suggest_recipes = resultSet.getString(3);
            String[] split = suggest_recipes.split(",");
            String dictionary_code = resultSet.getString(1);
            String status = resultSet.getString(2);
            for (int i = 0; i < split.length; i++) {
                Map<String, String> map = new HashedMap();
                map.put("dictionary_code", dictionary_code);
                map.put("status", status);
                map.put("suggest_recipe", split[i]);
                list.add(map);
            }
        }
        int y = 1;
        for (int i = 0; i < list.size(); i++) {
            Map<String, String> map = list.get(i);
            String suggest_recipe = map.get("suggest_recipe");
            String dictionary_code = map.get("dictionary_code");
            String status = map.get("status");
            String s = "select type from recipes where id = " + suggest_recipe;
            ResultSet resultSet1 = statement.executeQuery(s);
            while (resultSet1.next()) {
                String type = resultSet1.getString(1);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("suggest_recipe", suggest_recipe);
                jsonObject.put("type", type);
                jsonObject.put("dictionary_code", dictionary_code);
                jsonObject.put("status", status);
                System.out.println(jsonObject);
                jedis.hset("recipes", String.valueOf(y), jsonObject.toString());
                y ++;
            }
        }
        resultSet.close();
        statement.close();
        mySQLConnection.close();
    }




}
