/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package second.serviceDemo;

import java.util.*;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class JedisSentinelPoolImpl extends AbstractControllerService implements JedisSentinelPoolService {

    public static final PropertyDescriptor SENTINEL_HOSTNAME_PORT = new PropertyDescriptor
            .Builder().name("hostname and port")
            .displayName("hostname and port")
            .description("sentinel service(哨兵服务)hostname:port,多个时用“,”分隔")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password").displayName("password")
            .description("redis密码")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true).build();

    public static final PropertyDescriptor MASTER_NAME = new PropertyDescriptor.Builder()
            .name("master name").displayName("master name")
            .description("在sentinel中配置的master名")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    private JedisSentinelPool pool;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SENTINEL_HOSTNAME_PORT);
        props.add(PASSWORD);
        props.add(MASTER_NAME);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void OnEnabled(final ConfigurationContext context) throws InitializationException {

        String hostnamePort = context.getProperty(SENTINEL_HOSTNAME_PORT).getValue();
        String[] hostnamePorts = hostnamePort.split(",");
        Set<String> sentinels = new HashSet<>();

        for (int i = 0; i < hostnamePorts.length; i++) {
            sentinels.add(hostnamePorts[i]);
        }
        String masterName = context.getProperty(MASTER_NAME).getValue();

        String password = context.getProperty(PASSWORD).getValue();

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
        pool = new JedisSentinelPool(masterName, sentinels, config, password);
    }

    @OnDisabled
    public void shutdown() {
        if (pool != null) {
            pool.destroy();
        }
    }


    @Override
    public Jedis getJedis() {
        Jedis resource = pool.getResource();
        return resource;
    }
}
