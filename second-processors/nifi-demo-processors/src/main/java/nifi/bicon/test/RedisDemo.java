package nifi.bicon.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by NightWatch on 2018/4/13.
 */
public class RedisDemo {
    public static void main(String[] args) {

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

        sentinels.add("10.1.24.215:17003");

        System.out.println(sentinels);

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("masterTest", sentinels, config, "Kingleading");

        Jedis jedis = jedisSentinelPool.getResource();

        jedis.set("1", "1");

        System.out.println(jedis.get("1"));

        jedis.del("1");

        jedis.close();


    }

}
