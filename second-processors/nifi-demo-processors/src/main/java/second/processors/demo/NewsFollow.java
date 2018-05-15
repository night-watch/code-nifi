package second.processors.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import nifi.bicon.util.BigMap;
import nifi.bicon.util.Constant;
import nifi.bicon.util.JedisClusterUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.Test;
import redis.clients.jedis.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;


/**
 * Created by NightWatch on 2018/1/14.
 */
public class NewsFollow extends AbstractProcessor {

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

            String userId = String.valueOf(jsonObject.get("user_id"));

            String stampIds = String.valueOf(jsonObject.get("stamp_ids"));

            jedisCluster.hset("follow", userId, stampIds);

            session.transfer(flowFile, MY_RELATIONSHIP_SUCEESS);
        } catch (Exception e) {
            getLogger().error("新闻分类信息同步", e);
            session.transfer(flowFile, MY_RELATIONSHIP_FAILURE);
        }





    }



    @Test
    public void test1() {

        /*String s = ",";

        String[] split = s.split(",");

        System.out.println


        JSONArray j = JSONArray.fromObject(x);

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


        for (int i = 0; i < j.size(); i++) {
            JSONObject jsonObject = j.getJSONObject(i);

            String item = jsonObject.getString("item");
            if (!diseases.contains(item)) {
                continue;
            }
            String result = jsonObject.getString("result");

            try {

                Double.parseDouble(result);
            } catch (Exception e) {
                System.out.println(item + "\t\t\t" + result);

                System.out.println(jsonObject);
            }



        }*/



/*
        long time = 1511516725000L;

        java.sql.Date date = new Date(time);

        System.out.println(date);*/
        
        
        

/*
        int x = -1;

        int i = x / 4;

        System.out.println(i);*/

/*
        Map<String, String> m = new HashedMap();

        System.out.println(m.get("x"));*/

        /*int c = '-';

        System.out.println(c);*/

/*        String s = ",x";

        String[] split = s.split(",");

        System.out.println(split.length);

        System.out.println(61/20 +1);*/


        /*String s = "453,592,594,601,607,612,614,587,605,610,613,590,598,606,608,615,528,602,609,611,485,487,488,490,491,493";

        String s1 = "493,491,490,488,487,485,493,491,490,488,487,485,614,612,607,601,594,592,453,614,612,607,601,594,592,453";

        String[] split = s.split(",");

        String[] split1 = s1.split(",");

        for (int i = 0; i < split.length; i++) {
            boolean b = false;
            for (int j = 0; j < split1.length; j++) {

                if (split[i].equals(split1[j])) {
                    b = true;
                }
            }
            if (!b) {
                System.out.println(split[i]);
            }
        }*/


        /*ArrayList<Map<String, String>> list = new ArrayList<>();
        HashMap<String, String> map = new HashMap<>();
        map.put("asdf", "bgd");

        HashMap<String, String> map1 = new HashMap<>();
        map1.put("asdf", "bgd");

        list.add(map);


        System.out.println(list.contains(map1));*/



 /*       String content = "{\"action_type_x\":\"A\",\"create_date\":1516179245551,\"new_id\":\"32898\",\"stay_time\":\"0\",\"type\":0,\"user_id\":\"4d30aaa3f34946caa2cc71511897127a\"}";

        Map<String, String> map = (Map<String, String>) com.alibaba.fastjson.JSONObject.parse(content);

        String s = "afs,asdf";
        String[] split = s.split(",");

        System.out.println(split.length);*/

/*        List<String> list = new ArrayList<>();

        list.add("ASD");
        list.add("sdf");

        System.out.println(list.toString());*/

        /*JSONObject j = new JSONObject();

        j.put("xx", "");

        String xx = j.getString("xx");

        System.out.println(xx.length());*/


//        System.out.println(new Date(1506522375000L));


/*
        String s = "i\"m";

        System.out.println(s.replaceAll("\"", "\"\""));*/

/*
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(1024);

        config.setMaxIdle(200);

        config.setMaxWaitMillis(10000);

        config.setTestOnBorrow(true);*/


        String recommended = ",0,1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21,22,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,";
        LinkedList<Integer> indexList = new LinkedList<>();
        for (int i = 0; i < 400; i ++) {
            indexList.add(i);
        }

        LinkedList<Integer> list = new LinkedList<>();
        for (int i = 0; i < 40; i ++) {
            int index = (int)(Math.random() * indexList.size());

            Integer integer = indexList.get(index);

            if (!recommended.contains("," + integer + ",")) {
                list.add(integer);
                recommended = recommended.concat(integer + ",");
            } else {
                System.out.println("else");
                i --;
            }
            indexList.remove(index);
            //如果所有的下标已经随机完，则无需再循环，即使没有到20个也终止循环
            if (indexList.size() == 0) {
                break;
            }
        }

        System.out.println(indexList);

        System.out.println(list);

        System.out.println(recommended);


    }



    @Test
    public void test3() {


    }

    @Test
    public void test2() {

        Jedis jedis = new Jedis("10.1.24.216", 27000);
        jedis.auth("Kingleading");

        Map<String, String> map = jedis.hgetAll(Constant.OLD_NEWS_NOW);


        System.out.println(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

        System.out.println(map);

        for (int i = 0; i < 53547; i++) {
            String id = jedis.hget("yno" + i, "id");

            System.out.println(id);
            if (id == null) {
                System.out.println(i);
                break;
            }

        }

        /*for (int i = 0; i < 38243; i++) {

            String id = jedis.hget("yno" + i, "id");

            System.out.println(id);
            if (id == null) {
                System.out.println(i);
                break;
            }


        }*/

        /*for (int i = 0; i < 47270; i++) {
            Map<String, String> map1 = jedis.hgetAll("yno" + i);

            if (map1.get("id") == null) {
                System.out.println(i);
                break;
            }

//            System.out.println(map1);
        }*/

        /*System.out.println(jedis.hgetAll("xno" + 12855));

        for (int i = 0; i < 12855; i++) {
            Map<String, String> map1 = jedis.hgetAll("xno" + i);

            jedis.hmset("yno" + i, map1);
        }*/

        jedis.close();
    }


    @Test
    public void test() throws IOException, ClassNotFoundException, SQLException {
//        System.out.println(Constant.NO_BUG);

//        Jedis jedis = new Jedis("10.1.2.47", 27002);
//
//
//        jedis.auth("Kingleading");
//
//        jedis.set("he", "rld");
//
//        System.out.println(jedis.get("he"));

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

        jedis.set("test", "test");

        System.out.println(jedis.get("test"));

        jedis.del("test");



        /*jedis.hset(Constant.OLD_NEWS_NOW, "order_new_name", "xno");

        jedis.hset(Constant.OLD_NEWS_NOW, "order_now_name", "yno");


        System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));*/

//        26140   xno

        /*Set<String> keys = jedis.keys("*" + Constant.NEWS_ORDER_TODAY_ + "*");

        Iterator<String> iterator = keys.iterator();*/

        /*int count = 99999990 ;

        while (iterator.hasNext()) {
            String substring = iterator.next().substring(4);

            if (Integer.parseInt(substring) < count) {

                count = Integer.parseInt(substring);
            }


            System.out.println(count);
        }*/


        /*System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));

        System.out.println(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));




        for (int i = 36737; i < 571683; i++) {

            String id = jedis.hget(Constant.NEWS_ORDER_TODAY_, "id");

            if (id == null) {

                System.out.println(i);
                break;

            }

//            15838

            System.out.println(id);

        }



*/








        /*jedis.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, "122490");

        jedis.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NEW_NAME, "xno");

        jedis.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NOW_NAME, "yno");*/

        /*for (int i = 5113; i < 5114; i++) {
            jedis.hmset("xno" + i, jedis.hgetAll("yno" + i));
        }*/


        /*for (int i = 0; i < 26140; i++) {

            Map<String, String> map = jedis.hgetAll("xno" + i);

            if (map.size() > 6) {
                System.out.println(i);
                break;
            }


        }*/

        /*for (int i = 0; i < 26141; i++) {

            Map<String, String> map = jedis.hgetAll("yno" + i);

            if (map.size() < 5) {
                System.out.println(i);
                break;
            }

//            System.out.println(map);

        }*/

        /*for (int i = 0; i < 10000; i++) {
            System.out.println(jedis.hgetAll("yno" + i));
        }*/




/*        Map<String, String> map = new HashedMap();

        map.put(Constant.LENGTH, 0 + "");

        map.put(Constant.ORDER_NEW_NAME, "xno");
        map.put(Constant.ORDER_NOW_NAME, "yno");

        jedis.hmset(Constant.OLD_NEWS_NOW, map);

        System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));

        jedis.set(Constant.NEWS_ORDER_TODAY_COUNT, 0 + "");*/

        /*System.out.println(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

        System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));

        long time1 = new java.util.Date().getTime();
        System.out.println(time1);
        for (int i = 0; i < 90000; i++) {
            Map<String, String> map = jedis.hgetAll("yno" + i);
//            System.out.println(map);
            jedis.hmset("test" + i, map);
            jedis.del("test" + i);
        }

        long time2 = new java.util.Date().getTime();

        System.out.println(time2 - time1);*/


/*        Integer count = 0;

        resource.set("count", "0");

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10000; i++) {

                        *//*synchronized (count) {
                            count ++;
                        }*//*

                        resource.set("count", (Integer.parseInt(resource.get("count")) + 1) + "");
                    }
                }
            }).start();
        }

        while (true) {
            if (count == 100000) {
                break;
            }
        }

        System.out.println(resource.get("count"));*/

        jedis.close();

        jedisSentinelPool.close();




        /*System.out.println(jedis.get(Constant.NEWS_ORDER_TODAY_COUNT));

        System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));*/

/*        jedis.del("test");

        Map<String, String> map = new HashedMap();

        map.put(Constant.LENGTH, 0 + "");

        map.put(Constant.ORDER_NEW_NAME, "xno");
        map.put(Constant.ORDER_NOW_NAME, "yno");

        jedis.hmset(Constant.OLD_NEWS_NOW, map);

        System.out.println(jedis.hgetAll(Constant.OLD_NEWS_NOW));

        jedis.set(Constant.NEWS_ORDER_TODAY_COUNT, 0 + "");*/

//        jedis.close();





//        JedisClusterUtil jedisClusterUtil = new JedisClusterUtil("10.1.24.216", "17001,17002,17003,17004,17005,17006", "Kingleading");

/*        JedisClusterUtil jedisClusterUtil = new JedisClusterUtil("10.1.24.216", "27000", "Kingleading");

        JedisCluster j = jedisClusterUtil.getJedisCluster();*/

/*        for (int i = 0; i < 1000; i++) {

            if ("61157".equals(j.hget("yno" + i, "id"))) {

                System.out.println("\n\n\n" + j.hgetAll("yno" + i));
            }
        }
        for (int i = 0; i < 1000; i++) {

            if ("61157".equals(j.hget(Constant.NEWS_ORDER_TODAY_ + i, "id"))) {

                System.out.println(j.hgetAll(Constant.NEWS_ORDER_TODAY_ + i));
            }

        }*/



//        String hget = j.hget(Constant.RECOMMENDED, "13579e80a5e0481fafd61654b976b67a");

/*        String hget = ",+,+,+,59071,61193,59075,61134,59449,58212,59382,59450,59469,59080,61137,59132,59206,59700,59481,61141,59415,61151,59705,61138,59030,59403,61156,59420,59489,59445,59034,59388,59185,59074,59483,59488,59451,59471,59762,59052,59395,59166,59363,59374,45403,45620,44543,45643,44545,50212,45267,48974,45641,45327,50032,50528,45576,45552,45196,45238,45415,45259,44542,45621,48962,45241,45260,45271,45657,45291,45334,50529,50874,50317,50453,50526,50298,50100,48494,48958,45624,45639,48978,45617,45640,45647,45654,45205,45206,45207,45214,45217,45224,45237,45266,45328,48946,48966,50034,50037,50046,50278,45326,45637,59084,59198,61158,61121,59688,59177,59090,59181,59446,59037,59205,59047,59708,59430,59684,61130,61164,59020,61127,59526,59500,59005,59197,59055,61139,59431,59768,59461,59522,59368,59113,58200,59477,59703,59046,59691,59386,59713,59438,59097,45658,45678,48370,50451,50458,50459,50525,50937,50938,44869,44872,50090,50115,50522,44549,45197,44539,45054,45061,44471,44597,45494,50097,50098,44464,44465,44466,44467,44468,44469,44470,44472,44473,44511,44512,44537,44538,44540,44541,44544,44547,44548,44550,44551,44552,44553,44554,44555,44556,44557,44558,44559,44560,44562,44563,44585,44586,44587,44588,44589,59421,59160,59167,59028,59518,61187,61174,59405,59427,59048,59377,61123,59014,59026,59187,61148,59126,59707,59054,59204,59184,59128,59484,59534,59375,59510,58222,58217,61143,59476,59516,59180,59492,59183,59112,58204,59493,59709,59174,59117,44590,44591,44592,44593,44594,44595,44596,44598,44599,44600,44601,44602,44604,44606,44607,44613,44616,44618,44621,44622,44623,44630,44632,44633,44634,44635,44636,44637,44638,44639,44641,44642,44643,44644,44645,44646,44647,44648,44649,44651,44675,44680,44692,44698,44841,44844,44845,44846,44847,44848,44849,44850,44851,44852,44853,44854,44855,44857,44858,44859,59505,59383,59082,59078,59712,59485,59127,61128,59136,59503,59107,59089,59139,59023,59362,61155,59015,59050,59193,59025,59083,58221,61126,59696,59682,59130,59098,61142,59465,59178,59077,59142,59508,59027,58201,59172,59685,59497,59096,59394,44864,44866,44870,44871,44897,44898,44899,44906,44907,44908,44911,44914,44917,44918,44920,44923,44925,44926,44929,44930,44933,44934,44935,44938,44939,44940,44943,44944,44945,44948,44950,44951,44953,44955,44958,44961,44964,44967,44969,44971,44974,44975,44977,44979,45005,45063,45170,45215,45216,45218,45219,45220,45222,45227,45239,45240,45242,45258,45268,45269,61163,59512,59385,59459,59676,58205,59208,59690,59437,59133,59672,61186,59523,59701,58199,59455,61133,59086,59470,61153,59191,59515,61136,59487,59711,59370,59402,59472,59039,58218,59448,59429,59406,59379,59199,59168,59031,59116,59604,59473,45275,45279,45283,45285,45286,45287,45288,45289,45290,45292,45293,45304,45324,45325,45329,45333,45416,45417,45418,45419,45420,45421,45422,45423,45424,45425,45426,45427,45428,45429,45431,45444,45445,45446,45447,45448,45449,45450,45451,45453,45454,45455,45456,45457,45458,45459,45460,45461,45463,45469,45472,45475,45479,45482,45483,45484,45485,45487,45490,45491,59466,59458,59169,59202,61144,59134,59521,61167,59164,59171,59189,59426,59416,58224,59135,59479,59373,59022,59680,59060,59524,58213,59498,59434,59040,59454,59457,59073,59069,58206,59111,59425,59059,59464,59763,59109,59706,59462,59364,59533,45492,45493,45495,45496,45497,45498,45499,45500,45501,45502,45503,45504,45505,45506,45507,45508,45509,45510,45560,45561,45562,45573,45574,45575,45577,45578,45579,45580,45581,45582,45583,45584,45585,45586,45587,45588,45589,45590,45591,45592,45593,45594,45603,45604,45611,45613,45615,45618,45619,45622,45623,45625,45626,45627,45628,45629,45633,45634,45635,45636,59486,59674,59409,59165,59443,58208,59704,59369,59436,59085,61132,59013,59019,59424,59502,59210,59480,59125,59496,59766,59029,61129,59519,61122,59190,59387,59061,59456,59389,59422,59119,59687,59141,59110,59024,59410,59677,59138,59008,59371,45638,45645,45648,45649,45650,45651,45653,45656,48374,48376,48378,48380,48382,48384,48386,48388,48390,48392,48491,48944,48948,48950,48952,48954,48956,48961,48964,48968,50033,50035,50036,50038,50045,50047,50048,50049,50051,50052,50053,50054,50055,50071,50073,50074,50080,50081,50082,50083,50084,50085,50086,50087,50088,50089,50091,50092,50093,50094,50095,50096,59439,59504,61196,59072,59209,59091,59140,59433,61150,59106,59175,59391,61169,58203,59062,59124,58223,59400,59407,59087,59673,59003,59501,59423,59393,59491,59201,59463,59428,59397,59408,59380,59535,59372,58207,58216,59392,59200,59532,58202,50101,50102,50103,50106,50107,50110,50112,50113,50114,50116,50117,50118,50119,50120,50121,50133,50134,50143,50144,50146,50147,50148,50149,50150,50151,50152,50153,50154,50155,50156,50190,50191,50209,50216,50218,50219,50220,50221,50222,50223,50224,50225,50226,50227,50228,50253,50254,50255,50256,50261,50262,50263,50264,50265,50266,50267,50268,50269,50270,50271,61160,61184,59413,59118,59161,59207,59056,59170,59418,59384,59710,59683,59102,59099,59390,59412,59678,59051,58210,58220,59192,59295,59482,59076,59378,59063,59188,58209,59478,59104,59179,59440,59079,59365,59366,59460,59120,59435,59367,59411,50272,50273,50274,50276,50277,50279,50280,50284,50288,50291,50295,50296,50297,50305,50307,50308,50309,50310,50311,50312,50313,50314,50315,50318,50319,50320,50321,50322,50323,50325,50326,50328,50329,50330,50331,50332,50333,50334,50335,50337,50338,50339,50340,50341,50342,50343,50345,50346,50347,50348,50349,50350,50351,50352,50353,50354,50355,50357,50380,50381,59514,59529,59065,61172,61149,59294,59474,59094,59530,59679,59081,59108,59123,59376,59499,59070,59163,59035,59509,58225,59049,59176,58211,59194,59017,59173,59137,59064,59016,59068,59182,59021,59093,56850,59095,59053,50382,50383,50384,50385,50388,50389,50390,50391,50392,50393,50394,50396,50397,50398,50399,50400,50401,50402,50403,50404,50405,50406,50407,50408,50409,50410,50411,50412,50413,50414,50415,50416,50417,50418,50419,50420,50426,50428,50429,50430,50431,50432,50433,50434,50435,50436,50437,50438,50439,50440,50441,50442,50443,50444,50445,50446,50447,50448,50449,50450,50452,50455,50456,50457,61178,59475,59158,59511,59681,59686,61118,59675,59507,59536,59467,59203,59381,59520,59506,59186,59414,59432,59441,59162,59444,59417,59196,59088,59114,50460,50461,50462,50463,50464,50465,50466,50467,50468,50469,50470,50471,50472,50473,50474,50475,50476,50477,50478,50479,50480,50481,50482,50483,50484,50485,50486,50487,50489,50490,50491,50492,50493,50494,50495,50496,50497,50498,50499,50500,50501,50502,50503,50504,50506,50507,50511,50512,50513,50514,50515,50516,50519,50520,50521,50523,50530,50531,50532,50533,50534,50535,50536,50537,50538,50539,50540,50541,50542,50543,50544,50545,50546,50547,50865,61154,59419,59468,59495,59764,61135,59490,59131,59442,59447,59129,50866,50867,50872,50935,50936,50939,50940,50941,50942,50943,50944,50945,50946,50947,50948,50949,50950,50951,50952,50953,50954,50955,50956,50957,50958,50959,50960,50961,50972,50973,50975,50976,50977,50978,50979,50983,50985,50987,50989,50991,50992,50994,50995,50997,52461,52522,52557,52560,52563,52565,52567,52569,52571,52574,54525,54527,54530,54532,54534,54536,54559,54561,54566,54727,59517,61162,59761,59513,61145,61140,59494,59689";

        System.out.println(hget);

        System.out.println(hget.contains(",56850,"));

        String[] split = hget.split(",");


        System.out.println(split.length);

        for (int i = 0; i < split.length; i++) {

            if (split[i].equals("56850")) {
                System.out.println(i);
            }


        }*/


//        System.out.println(j.hgetAll("relation"));


//        Map<String, String> map = j.hgetAll(Constant.NEWID_TO_ORDERID);

/*        j.del(Constant.NEWID_TO_ORDERID);

//        System.out.println(map.size());


        for (int i = 0; i < 986; i++) {
            System.out.println(j.hgetAll(Constant.NEWS_ORDER_TODAY_ + i));
            j.del(Constant.NEWS_ORDER_TODAY_ + i);
        }

        j.set(Constant.NEWS_ORDER_TODAY_COUNT, 0 + "");*/


/*
        String s = j.hget(Constant.RECOMMENDED, "0528ac4cf42943afa192aa0b622a9318");

        System.out.println(s.split(",").length);*/


//        System.out.println(j.hgetAll(Constant.RECOMMENDED));

//        j.del(Constant.RECOMMENDED);

/*        int x = 0;

        for (int i = 0; i < 60643; i++) {
            Map<String, String> map = j.hgetAll("xno" + i);
            if (!"".equals(map.get("writer"))) {
                System.out.println(map);
                x ++;
            }

        }

        System.out.println(x);*/

//        j.set("test", "test");

//        System.out.println(j.hgetAll("xno41304"));
//        System.out.println(j.get("test"));


//        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));
//        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

/*        for (int i = 0; i < 6761; i++) {
            System.out.println(j.hgetAll("xno" + i));
//            j.del("xno" + i);
        }*/

 //        j.set(Constant.NEWS_ORDER_TODAY_COUNT, 0 + "");

//        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");

/*
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://10.1.2.49:3306/news", "root", "123456");


        String sql = "replace INTO news_info_copy (action_type_x, id, title, image_url, type, tags, src, writer, url, read_count, comment_count, collection_count, create_date, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);


        Map<String, String> oldNewsNow = j.hgetAll(Constant.OLD_NEWS_NOW);

        System.out.println(oldNewsNow);

        int length = Integer.parseInt(oldNewsNow.get(Constant.LENGTH));

        String nowName = oldNewsNow.get(Constant.ORDER_NOW_NAME);

        int todayCount = Integer.parseInt(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

        for (int i = 0; i < length; i++) {
            Map<String, String> news = j.hgetAll(nowName + i);
            String commentCount = news.get("comment_count");
            String src = news.get("src");
            String title = news.get("title");
            String type = news.get("type");
            String url = news.get("url");
            String tags = news.get("tags");
            String actionTypeX = news.get("action_type_x");
            String collectionCount = news.get("collection_count");
            String imageUrl = news.get("image_url");
            String id = news.get("id");
            String writer = news.get("writer");
            java.sql.Date createDate = new Date(Long.parseLong(news.get("create_date")));
            String readCount = news.get("read_count");
            String allCount = news.get("all_count");
            preparedStatement.setString(1, actionTypeX);
            preparedStatement.setString(2, id);
            preparedStatement.setString(3, title);
            preparedStatement.setString(4, imageUrl);
            preparedStatement.setString(5, type);
            preparedStatement.setString(6, tags);
            preparedStatement.setInt(7, Integer.parseInt(src));
            preparedStatement.setString(8, writer);
            preparedStatement.setString(9, url);
            preparedStatement.setInt(10, Integer.parseInt(readCount));
            preparedStatement.setInt(11, Integer.parseInt(commentCount));
            preparedStatement.setInt(12, Integer.parseInt(collectionCount));
            preparedStatement.setDate(13, createDate);
            preparedStatement.setInt(14, Integer.parseInt(allCount));
            System.out.println(imageUrl);
            System.out.println(id);
            preparedStatement.execute();
        }

        for (int i = 0; i < todayCount; i++) {
            Map<String, String> news = j.hgetAll(Constant.NEWS_ORDER_TODAY_ + i);
            String commentCount = news.get("comment_count");
            String src = news.get("src");
            String title = news.get("title");
            String type = news.get("type");
            String url = news.get("url");
            String tags = news.get("tags");
            String actionTypeX = news.get("action_type_x");
            String collectionCount = news.get("collection_count");
            String imageUrl = news.get("image_url");
            String id = news.get("id");
            String writer = news.get("writer");
            java.sql.Date createDate = new Date(Long.parseLong(news.get("create_date")));
            String readCount = news.get("read_count");
            String allCount = news.get("all_count");
            preparedStatement.setString(1, actionTypeX);
            preparedStatement.setString(2, id);
            preparedStatement.setString(3, title);
            preparedStatement.setString(4, imageUrl);
            preparedStatement.setString(5, type);
            preparedStatement.setString(6, tags);
            preparedStatement.setInt(7, Integer.parseInt(src));
            preparedStatement.setString(8, writer);
            preparedStatement.setString(9, url);
            preparedStatement.setInt(10, Integer.parseInt(readCount));
            preparedStatement.setInt(11, Integer.parseInt(commentCount));
            preparedStatement.setInt(12, Integer.parseInt(collectionCount));
            preparedStatement.setDate(13, createDate);
            preparedStatement.setInt(14, Integer.parseInt(allCount));
            System.out.println(id);
            preparedStatement.execute();
        }
*/



        /*for (Map.Entry<String, String> e : map.entrySet()) {

            if (e.getKey().equals("36015")) {

                System.out.println(e.getKey() + "\t\t\t" + e.getValue());
            }

            i ++;

        }*/



//        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));
//
//        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

//        System.out.println(j.hgetAll("xno1"));

        /*for (int i = 0; i < 4622; i++) {

            *//*Map<String, String> map = j.hgetAll("xno" + i);

            String image_url = map.get("image_url");

            if (image_url.length() > 10) {
                System.out.println(image_url);
            }*//*

            j.del("yno" + i);


        }*/

//        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");








//        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

//        j.del("recommended");


        /*Map<String, String> map = j.hgetAll("LRTZ_JKZB_DGC");


        System.out.println(map);*/



/*        Map<String, String> map = j.hgetAll(Constant.ALL_LIMIT);

        for (Map.Entry<String, String> s : map.entrySet()) {
//            System.out.println(s.getValue() + "\t\t\t" +s.getKey());

            String value = s.getValue();

            String[] split = value.split(",");
            if (split.length == 1) {
                System.out.println(value);
            }


        }*/





/*        Map<String, List<Map<String, String>>> venueINFOs = new HashedMap();

        Map<String, List<Map<String, String>>> coachINFOs = new HashedMap();


        Set<String> venueClassifications = j.smembers(Constant.VENUE_CLASSIFICATIONS);
        for (String venueClassification : venueClassifications) {
            List<Map<String, String>> list = new ArrayList<>();
            Set<String> venueIDes = j.smembers(venueClassification);
            for (String venueID : venueIDes) {
                list.add(j.hgetAll("venueID-" + venueID));
            }
            venueINFOs.put(venueClassification, list);
        }

        Set<String> coachClassifications = j.smembers(Constant.COACH_CLASSIFICATIONS);
        for (String coachClassification : coachClassifications) {
            List<Map<String, String>> list = new ArrayList<>();
            Set<String> coachIDes = j.smembers(coachClassification);
            for (String coachID : coachIDes) {
                list.add(j.hgetAll("coachID-" + coachID));
            }
            coachINFOs.put(coachClassification, list);
        }*/





//        System.out.println(venueINFOs);

//        System.out.println(coachINFOs);



/*        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));

        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

//        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");


        for (int i = 0; i < 40; i++) {
//            System.out.println(j.hgetAll("xno" + i));

            j.del("xno" + i);


        }


        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");*/

/*        for (int i = 0; i < 4500; i++) {

//            System.out.println(j.hgetAll(Constant.NEWS_ORDER_TODAY_ + i));
            j.del(Constant.NEWS_ORDER_TODAY_ + i);
        }

//        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");
//
        j.set(Constant.NEWS_ORDER_TODAY_COUNT, 0 + "");*/

        /*Set<String> recipeClassifications = j.smembers(Constant.RECIPE_CLASSIFICATIONS);
        System.out.println(recipeClassifications);
        for (String recipeClassification : recipeClassifications) {
            System.out.println(j.lrange(recipeClassification, 0, -1));
        }*/

        /*j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");

        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));

        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

        j.del(Constant.NEWID_TO_ORDERID);*/

        /*System.out.println(j.hgetAll(Constant.NEWID_TO_ORDERID));*/

/*
        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));

        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");

        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));

        for (int i = 0; i < 606; i++) {

            System.out.println(j.hgetAll("yno" + i));
//            j.del("yno" + i);
        }
*/




        /*Set<String> mainProjectNames = j.smembers(Constant.MAIN_PROJECT_NAMES);

        System.out.println(mainProjectNames);
        for (String projectName : mainProjectNames) {
            Map<String, String> info = j.hgetAll(projectName + "-info");
            System.out.println(info);
        }*/


        /*System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));

        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));


        for (int i = 0; i < 608; i++) {
            Map<String, String> map = j.hgetAll("xno" + i);

            if (map.size() < 3) {
                System.out.println(map);
            }

        }*/

/*        Map<String, String> map = j.hgetAll(Constant.NEWID_TO_ORDERID);

        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println("key"+entry.getKey());
            System.out.println("value"+entry.getValue());
        }
        System.out.println(map.size());*/


/*        for (int i = 0; i < 33424; i++) {
            *//*Map<String, String> map = j.hgetAll("yno" + i);

            if (map.size() < 3 ){
                System.out.println(map);
            }*//*
            j.del("yno" + i);

        }*/



/*        Map<String, String> map = j.hgetAll(Constant.USER_LRTZ + "9c76eaba66194caa9484a3df4d5922ec");

        Calendar timeZero = Calendar.getInstance();
        timeZero.set(Calendar.HOUR_OF_DAY, 0);
        timeZero.set(Calendar.SECOND, 0);
        timeZero.set(Calendar.MINUTE, 0);
        timeZero.set(Calendar.MILLISECOND, 0);
        long timeInMillis = timeZero.getTimeInMillis();

        int x = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            JSONObject jsonObject = JSONObject.fromObject(entry.getValue());

            x ++;
            int status = jsonObject.getInt("status");

            System.out.println("status"+status);

//            System.out.println(x);
            if (timeInMillis < jsonObject.getLong("create_date") ) {
                System.out.println(jsonObject);
            }

        }

        System.out.println(x);*/
/*        j.set(Constant.NEWS_ORDER_TODAY_COUNT, 12000 + "");

        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 9999 + "");

        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));


        for (int i = 10000; i < 11000; i++) {
            Map<String, String> map = j.hgetAll("xno" + i);
            if (map.size() < 3) {
                System.out.println(i);
                System.out.println(map);
            }

        }*/



       /* System.out.println(j.hgetAll(Constant.NEWID_TO_ORDERID));*/

/*        Map<String, String> oldNewsNow = new HashedMap();*/

/*        System.out.println(j.smembers(Constant.USER_TARGET_SET));

        System.out.println(j.hgetAll(Constant.USER_LRTZ + "ba5a451751014473871fed6bc89f1ecf"));*/

/*        oldNewsNow.put(Constant.ORDER_NEW_NAME, "xno");

        oldNewsNow.put(Constant.ORDER_NOW_NAME, "yno");

        oldNewsNow.put(Constant.LENGTH, 10000 + "");

        j.hmset(Constant.OLD_NEWS_NOW, oldNewsNow);

        oldNewsNow = j.hgetAll(Constant.OLD_NEWS_NOW);*/

/*        j.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NOW_NAME, "yno");

        j.hset(Constant.OLD_NEWS_NOW, Constant.ORDER_NEW_NAME, "xno");*/


/*        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));

        System.out.println(j.get(Constant.NEWS_ORDER_TODAY_COUNT));*/


        /*j.hincrBy("yn", "all_count", 1);*/

/*        j.hset(Constant.OLD_NEWS_NOW, Constant.LENGTH, 0 + "");*/


/*
        System.out.println(j.hgetAll(Constant.OLD_NEWS_NOW));


        int x = 0;

        for (int i = 0; i < 10000; i++) {

            Map<String, String> map = j.hgetAll("xno" + i);
            if (map.size() < 3) {
                System.out.println(map);

                x ++;
                System.out.println(i);
            }
        }
        System.out.println(x);
*/






/*        map.put("create_date", Long.valueOf(map.get("create_date")) - 3*24*60*60*1000 + "");

        j.hmset("yno0", map);*/

/*
        for (Map.Entry<String, String> e : map.entrySet()) {
            System.out.println(e.getKey());
            System.out.println(e.getValue());
        }
*/





/*        Map<String, String> map = j.hgetAll(Constant.USER_LRTZ + "9c76eaba66194caa9484a3df4d5922ec");

        for (Map.Entry<String, String> e : map.entrySet()) {
            System.out.println(e.getKey());
        }*/



/*        System.out.println(j.hget("yno1", "all_count"));

        Map<String, String> map = j.hgetAll("formulas");

        for (Map.Entry<String, String> e : map.entrySet()) {
            System.out.println(e.getKey());
            System.out.println(e.getValue());
        }*/


        /*System.out.println(j.hgetAll(Constant.NEWID_TO_ORDERID));*/

        /*j.hset("relation", "BZZB_RC_STEP", "LRTZ_JKZB_STEP");*/

/*        System.out.println(j.hgetAll("LRTZ_JKZB_BMI"));

        System.out.println(j.hget("relation", "BZZB_RC_HS"));*/

/*        Map<String, String> map = j.hgetAll("relation");

        for (Map.Entry<String, String> entry : map.entrySet()) {

            Map<String, String> map1 = j.hgetAll(entry.getValue());

            if (Integer.valueOf(map1.get("priority")) > 3){
                System.out.println(entry.getValue());
            }

            System.out.println(entry.getValue() + "=" + entry.getKey());
        }*/


//        System.out.println(j.hgetAll("yno1"));


//        j.close();



    }


}
