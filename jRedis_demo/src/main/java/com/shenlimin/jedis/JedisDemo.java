package com.shenlimin.jedis;

import redis.clients.jedis.Jedis;

public class JedisDemo {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.56.100", 6379);
        jedis.auth("123456");

        String ping = jedis.ping();
        System.out.println(ping);


        jedis.set("name", "shenlimin");
        System.out.println(jedis.get("name"));

    }
}
