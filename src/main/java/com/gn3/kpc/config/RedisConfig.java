package com.gn3.kpc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@PropertySource("classpath:application.properties")
public class RedisConfig {
    @Value("${redis.host}")
    private String host;

    @Value("${redis.port}")
    private int port;

    @Value("${redis.password}")
    private String pw;

    @Bean
    public JedisPoolConfig jedisPoolConfig(){
        return new JedisPoolConfig();
    }

    @Bean
    public JedisPool jedisPool(){
        return new JedisPool(jedisPoolConfig(), host, port, 1000, pw);
    }
}
