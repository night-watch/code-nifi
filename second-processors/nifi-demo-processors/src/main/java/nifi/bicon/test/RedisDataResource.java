package nifi.bicon.test;

import nifi.bicon.util.Constant;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * Created by NightWatch on 2018/2/2.
 */
public class RedisDataResource {

    private static ThreadLocal<ShardedJedis> jedisLocal = new ThreadLocal<ShardedJedis>();
    private static ShardedJedisPool pool;
    static {
        pool = RedisTest.getShardedJedisPool();
    }

    public  ShardedJedis getClient() {
        ShardedJedis jedis = jedisLocal.get();
        if (jedis == null) {



            jedis = pool.getResource();
            jedisLocal.set(jedis);
        }
        return jedis;
    }

    //关闭连接
    public void returnResource() {
        ShardedJedis jedis = jedisLocal.get();
        if (jedis != null) {
            pool.destroy();
            boolean closed = pool.isClosed();
            jedisLocal.set(null);
        }
    }

    @Test
    public void test() {

        ShardedJedis resource = getClient();

        System.out.println(resource.get("he"));

        resource.set("aaaa", "bbbbb");

        System.out.println(resource.get("aaaa"));






        /*System.out.println(resource.hgetAll(Constant.OLD_NEWS_NOW));

        String s = resource.get(Constant.NEWS_ORDER_TODAY_COUNT);

        System.out.println(s);*/


        resource.close();




/*        RedisDataResource redisDataResource = new RedisDataResource();

        ShardedJedis client = redisDataResource.getClient();

        String s = client.get(Constant.NEWS_ORDER_TODAY_COUNT);

        System.out.println(s);


        redisDataResource.returnResource();

        ShardedJedis client1 = redisDataResource.getClient();

        System.out.println(client1.get(Constant.NEWS_ORDER_TODAY_COUNT));
        System.out.println(client.get(Constant.NEWS_ORDER_TODAY_COUNT));*/
    }


}
