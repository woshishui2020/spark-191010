package com.atguigu.springboot;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisTest {

    @Autowired
    private RedisTemplate<Object,Object> redisTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void test() {

        ValueOperations<Object, Object> operations = redisTemplate.opsForValue();

        operations.set("school","shangguigu");

        System.out.println(operations.get("school"));

    }


    @Test
    public void test2() {

        Set<String> keys = stringRedisTemplate.keys("*");

        for (String key : keys) {
            System.out.println(key);
        }


    }
}
