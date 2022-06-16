package com.shenlimin.jedis;

import redis.clients.jedis.Jedis;

/**
 * @author shenlimin
 * 需求
 * 1、输入手机号，生成6位随机的验证码，2分钟内有效
 * 2、输入验证码，验证码是否正确，验证码是否过期
 * 3、每次手机号每天只能发送3次验证码
 */
public class Demo2Jedis {

    public static void main(String[] args) {
        Jedis jedis = JedisUtil.getJedis();

        String phone = "1818818888881";
        String code = getRandomNumber();
        //存储验证码
        String setex = jedis.setex(phone, 120, code);
        System.out.println(setex);

        //获取验证码
        String get = jedis.get(phone);
        System.out.println(get);

        //验证验证码第一次
        checkCode(jedis, phone, "123456");
        //验证验证码第二次
        checkCode(jedis, phone, code);


    }


    //验证验证码
    public static void checkCode(Jedis jedis, String phone, String code) {
        int count = getCount(jedis, phone);
        if (count > 3) {
            System.out.println("验证码超过3次,请明天再试!");
        } else {
            String check = jedis.get(phone);
            if (code.equals(check)) {
                jedis.del(phone);
                System.out.printf("\n验证码:%s正确", code);
            } else {
                System.out.printf("\n验证码:%s错误,还有%d次机会", code, 3 - count);
            }
            incr(jedis, phone);
        }


    }

    //验证次数增加
    public static void incr(Jedis jedis, String phone) {
        jedis.incr(phone + "_count");
    }


    //判断验证次数是否超过3次
    public static int getCount(Jedis jedis, String phone) {
        String count = jedis.get(phone + "_count");
        if (count == null) {
            return 0;
        }
        return Integer.parseInt(count);
    }


    //生成6位随机数
    private static String getRandomNumber() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            sb.append((int) (Math.random() * 10));
        }
        return sb.toString();
    }


}
