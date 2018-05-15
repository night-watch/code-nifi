package nifi.bicon.test;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by NightWatch on 2018/2/2.
 */
public class RedisTest {

    private static ShardedJedisPool pool;
    private static int MAXTOTAL=300;
    private static int MAXIDLE=200;
    private static int MINIDEL=10;
    private static int MAXWAIRMILLIS=1000;
    private static Boolean TESTONBORROW=true;
    private static Boolean TESTONRETURN=false;
    private static Boolean TESTWHILEIDLE=false;

    static {
        try {

            JedisPoolConfig config = initConfig();


            List<JedisShardInfo> shards = new ArrayList<>();

            String host = "10.1.2.47:27000:Kingleading,10.1.2.47:27001:Kingleading,10.1.2.47:27002:Kingleading";

            Set<String> hosts = init(host);

            for (String hs : hosts) {
                String[] values = hs.split(":");
                JedisShardInfo shard = new JedisShardInfo(values[0], Integer.parseInt(values[1]));


                if (values.length > 2) {
                    shard.setPassword(values[2]);
                }

                shards.add(shard);

            }



            pool = new ShardedJedisPool(config, shards);



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JedisPoolConfig initConfig(){
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(MAXTOTAL);
        config.setMaxIdle(MAXIDLE);
        config.setMinIdle(MINIDEL);
        config.setMaxWaitMillis(MAXWAIRMILLIS);
        config.setTestOnBorrow(TESTONBORROW);
        config.setTestOnReturn(TESTONRETURN);
        config.setTestWhileIdle(TESTWHILEIDLE);
        return config;
    }

    private static Set<String> init(String values){
        if(StringUtils.isBlank(values)){
            throw new NullPointerException("redis host not found");
        }
        Set<String> paramter=new HashSet<>();
        String[] sentinelArray=values.split(",");
        for(String str:sentinelArray){
            paramter.add(str);
        }
        return paramter;
    }

    public static ShardedJedisPool getShardedJedisPool(){
        return pool;
    }


    @Test
    public void test() {
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("a", "b");

        jsonObject.put("a", "c");


        System.out.println(jsonObject);


    }

}
