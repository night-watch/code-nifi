package nifi.bicon.util;

import redis.clients.jedis.Jedis;

/**
 * Created by NightWatch on 2018/3/6.
 */
public class JedisCreater {
    public static Jedis create(String hostname, int port, String password) {

        Jedis jedis = new Jedis(hostname, port);

        jedis.auth(password);

        return jedis;
    }
}
