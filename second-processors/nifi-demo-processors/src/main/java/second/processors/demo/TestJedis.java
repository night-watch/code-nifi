package second.processors.demo;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.fastjson.parser.ParserConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.codehaus.jackson.map.util.JSONPObject;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Kingleading on 2017/12/29.
 */
public class TestJedis {
    private static JedisCluster jedisCluster = null;

    static {
        try {
            Set<HostAndPort> hostAndPortSet = new HashSet<HostAndPort>();
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17001));
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17002));
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17003));
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17004));
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17005));
            hostAndPortSet.add(new HostAndPort("10.1.24.216", 17006));

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(2);

            jedisCluster = new JedisCluster(hostAndPortSet, 5000, 5000, 3, "Kingleading", jedisPoolConfig);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Test
    public void test() {
//        System.out.println(jedisCluster.get("key1"));
//        System.out.println(jedisCluster.get("key2"));
//        System.out.println(jedisCluster.get("key3"));
//        System.out.println(jedisCluster.get("key4"));
//        System.out.println(jedisCluster.get("key5"));
//        System.out.println(jedisCluster.get("key6"));
//        System.out.println(jedisCluster.get("key111"));
//
//        jedisCluster.append("key1", "value111");
//        System.out.println(jedisCluster.get("key1"));
//
//        System.out.println(jedisCluster.bitcount("key1"));
//
//        jedisCluster.set("key1", "value1");

        jedisCluster.hset("user_1", "newsIds", "1,2,3+4,5,6+7");
        jedisCluster.hset("user_1", "LRTZ_XY", getIndexResult("LRTZ_XY", 129));
        jedisCluster.hset("user_1", "LRTZ_XT", getIndexResult("LRTZ_XT", 8));
        jedisCluster.hset("user_1", "LRTZ_XZ", getIndexResult("LRTZ_XZ", 5));


        jedisCluster.hset("news_1", "title", "华为年终奖");
        jedisCluster.hset("news_1", "image_url", "a.jpg");
        jedisCluster.hset("news_1", "comment_count", "23");
        jedisCluster.hset("news_1", "all_count", "46");


        jedisCluster.hincrBy("news_1", "comment_count", 1);
        jedisCluster.hincrBy("news_1", "all_count", 2);
        System.out.println(jedisCluster.hget("news_1", "comment_count"));
        System.out.println(jedisCluster.hget("news_1", "all_count"));

        System.out.println(jedisCluster.hget("news", "news001"));
        System.out.println(jedisCluster.hkeys("news").getClass());
        System.out.println(jedisCluster.hvals("news").getClass());
        System.out.println(jedisCluster.hgetAll("news").getClass());

        Map<String, String> hm = new HashMap<String, String>();
        hm.put("hello", "world");
        hm.put("class", "Object");
        jedisCluster.hmset("map", hm);

        System.out.println(jedisCluster.hget("map", "hello"));
        System.out.println(jedisCluster.hgetAll("map"));
        System.out.println(jedisCluster.hmget("map", "class"));
        System.out.println(jedisCluster.hmget("map", new String[]{"hello", "class"}));
    }

    public String getIndexResult(String indexName, int indexValue){
        /**
         {
         "indexName":"体重",
         "indexCode":"LRTZ_JKZB_TZ",
         "indexValue":"52.0",
         "indexUnit":"kg",
         "status":-1,
         "highContent":"您的体重已经超出了平均值，请注意保持身材。",
         "lowContent":"您太瘦了，多补充营养，才能更健康。",
         "priority":1
         }
         */
        return "{\"content\":\"json\"}";
    }


//    @Test
//    public void testJsonCases(){
//        Map<String,Object> map = new HashMap<String, Object>();
//        map.put("LRTZ_XY","129");
//        map.put("LRTZ_XT",8);
//
//
//
//        String jsonStr = JSONObject.toJSONString(map);
//
//        System.out.println(jsonStr);
//
//        System.out.println(JSONObject.toJSON(jsonStr).getClass());
//        System.out.println(JSONObject.parse(jsonStr, ParserConfig.getGlobalInstance()).getClass());
//
//        Map<String, String> map2 = (Map<String, String>) JSONObject.parse(jsonStr);
//        System.out.println(map2.get("LRTZ_XY"));
//        System.out.println(JSONObject.toJSONString(map2));
//
//        Map<String, String> map3 = JSONObject.parseObject(jsonStr, Map.class);
//
//        System.out.println(map3.get("LRTZ_XY"));
//    }

//    @Test
//    public void testLittleJsonBean(){
//        Map<String,Object> map = new HashMap<String, Object>();
//        map.put("name","Kingleading");
//        map.put("age",31);
//        map.put("remark", "hello world");
//
//        String jsonStr = JSONObject.toJSONString(map);
//        System.out.println(jsonStr);
//
//        JavaBean jb = JSONObject.parseObject(jsonStr, JavaBean.class);
//        System.out.println(jb.getName());
//
//        System.out.println(JSONObject.toJSONString(jb));
//    }

    @Test
    public void initNewsOrder(){
        jedisCluster.hset(X_PRE_NEWS_ORDER + "0", "id", "id_9");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "0", "title", "title_9");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "0", "read_count", "1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "0", "comment_count", "1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "0", "all_count", "3");

        jedisCluster.hset(X_PRE_NEWS_ORDER + "1", "id", "id_1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "1", "title", "title_1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "1", "read_count", "1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "1", "comment_count", "2");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "1", "all_count", "5");


        jedisCluster.hset(X_PRE_NEWS_ORDER + "2", "id", "id_5");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "2", "title", "title_5");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "2", "read_count", "2");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "2", "comment_count", "1");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "2", "all_count", "4");

        jedisCluster.hset(X_PRE_NEWS_ORDER + "3", "id", "id_7");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "3", "title", "title_7");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "3", "read_count", "2");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "3", "comment_count", "3");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "3", "all_count", "8");

        jedisCluster.hset(X_PRE_NEWS_ORDER + "4", "id", "id_4");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "4", "title", "title_4");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "4", "read_count", "2");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "4", "comment_count", "2");
        jedisCluster.hset(X_PRE_NEWS_ORDER + "4", "all_count", "6");


//        Map<String, String> hm = (Map<String, String>)JSONObject.parse(jsonStr);
//        hm.put("all_count", "0");



        jedisCluster.set(PRE_NEWS_ID_ORDER + "id_9", "0");
        jedisCluster.set(PRE_NEWS_ID_ORDER + "id_1", "1");
        jedisCluster.set(PRE_NEWS_ID_ORDER + "id_5", "2");
        jedisCluster.set(PRE_NEWS_ID_ORDER + "id_7", "3");
        jedisCluster.set(PRE_NEWS_ID_ORDER + "id_4", "4");
    }


    private static long oldLen;
    private static String X_PRE_NEWS_ORDER = "xno_";
    private static String Y_PRE_NEWS_ORDER = "yno_";
    private static String NOW_PRE_NEWS_ORDER = X_PRE_NEWS_ORDER;
    private static String NEW_PRE_NEWS_ORDER = Y_PRE_NEWS_ORDER;
    private static String PRE_NEWS_ID_ORDER = "nid_";

    @Test
    public void redoOrder(){
        long[] a = new long[5];
        long[] b = new long[5];

        for(int i = 0; i < a.length; i++){
            a[i] = jedisCluster.hincrBy(NOW_PRE_NEWS_ORDER + i, "all_count", 0);
            b[i] = i;
        }

        for(int i = 0; i < a.length; i++){
            for(int j = 0; j < a.length - 1 - i; j++){
                if(a[j] < a[j + 1]){
                    long tmp = a[j];
                    a[j] = a[j + 1];
                    a[j + 1] = tmp;

                    tmp = b[j];
                    b[j] = b[j + 1];
                    b[j + 1] = tmp;
                }
            }
        }

        for(int i = 0; i < a.length; i++){
//            jedisCluster.rename("news_order_" + b[i], "news_order_x_" + i);
            jedisCluster.hmset(NEW_PRE_NEWS_ORDER + i, jedisCluster.hgetAll(NOW_PRE_NEWS_ORDER + b[i]));
            jedisCluster.set(PRE_NEWS_ID_ORDER + jedisCluster.hget(NOW_PRE_NEWS_ORDER + b[i], "id"), i + "");
            jedisCluster.del(NOW_PRE_NEWS_ORDER + b[i]);
        }


        for(int i = 0;i < a.length; i++){
            System.out.println(i + "\t" + jedisCluster.hgetAll(NEW_PRE_NEWS_ORDER + i).get("all_count") + "\t"
                    + jedisCluster.hget(NEW_PRE_NEWS_ORDER + i, "id") + "\t" + jedisCluster.get(PRE_NEWS_ID_ORDER + jedisCluster.hget(NEW_PRE_NEWS_ORDER + i, "id")));

        }
    }


    @Test
    public void dealReadLogs(){
        String newsId = "1";
        String type = "1/2/3";
        // insert to db
        if("1".equals(type)){
            jedisCluster.hincrBy(jedisCluster.get(newsId), "read_count", 1);
            jedisCluster.hincrBy(jedisCluster.get(newsId), "all_count", 1);
        }else if("2".equals(type)){
            jedisCluster.hincrBy(jedisCluster.get(newsId), "comment_count", 1);
            jedisCluster.hincrBy(jedisCluster.get(newsId), "all_count", 2);
        }else if("3".equals(type)){
            jedisCluster.hincrBy(jedisCluster.get(newsId), "collection_count", 1);
            jedisCluster.hincrBy(jedisCluster.get(newsId), "all_count", 3);
        }

    }
}

class JavaBean implements Serializable{
    private String name;
    private int age;

    public JavaBean(){}

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
}
