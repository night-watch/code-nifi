package nifi.bicon.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by NightWatch on 2018/1/12.
 */
public class JedisClusterUtil {

    private JedisCluster jedisCluster;

    private Set<HostAndPort> hostAndPorts;

    public JedisClusterUtil(String hostname, String ports, String password) {

        hostAndPorts = new HashSet<>();
        String[] port = ports.split(",");
        if (port == null || port.length == 0) {
            return;
        }
        for (int i = 0; i < port.length; i ++) {
            hostAndPorts.add(new HostAndPort(hostname, Integer.parseInt(port[i])));
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxIdle(2);
        jedisCluster = new JedisCluster(hostAndPorts, 5000, 5000, 3, password, jedisPoolConfig);


    }

    public void close() throws IOException {
        this.jedisCluster.close();
    }

    public JedisCluster getJedisCluster() {
        return this.jedisCluster;
    }
}
