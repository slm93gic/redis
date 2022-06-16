package com.shenlimin.jedis;

import redis.clients.jedis.Jedis;

public class JedisUtil {

    private static Jedis jedis;

    public static Jedis getJedis() {
        if (jedis == null) {
            Jedis MyJedis = new Jedis("192.168.56.100", 6379);
            MyJedis.auth("123456");
            jedis = MyJedis;
        }
        return jedis;
    }


}

