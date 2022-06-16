package com.sunsheen.wmmd.scsdata.scscfg.cfg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

@SuppressWarnings("all")
public class RedisUtils {
  private static Logger logger = LoggerFactory.getLogger(RedisUtils.class);

  private static Jedis jedis;

  static {
    try {
      if (jedis == null) {
        Jedis MyJedis = new Jedis("192.168.56.100", 6379);
        MyJedis.auth("123456");
        jedis = MyJedis;

      }
    } catch (Exception e) {
      logger.error("Jedis pool create fail!!!");
      e.printStackTrace();
    }
  }

  /**
   * 获取Jedis实例
   *
   * @return
   */
  public static synchronized Jedis getJedis() {
    return jedis;
  }

  /**
   * 获取redis键值-object
   *
   * @param key
   * @return
   */
  public static Object getObject(String key) {
    try {
      byte[] bytes = jedis.get(key.getBytes());
      return SerializeUtils.unserialize(bytes);
    } catch (Exception e) {

    } finally {
      jedis.close();
    }
    return null;
  }

  /**
   * 设置redis-object
   *
   * @param key
   * @param value
   * @return
   */
  public static String setObject(String key, Object value) {
    try {
      if (existsObject(key)) {
        delkeyObject(key);
      }

      return jedis.set(key.getBytes(), SerializeUtils.serialize(value));
    } catch (Exception e) {
      logger.error("setObject设置redis键值异常:key=" + key + " value=" + value + " cause:" + e.getMessage());
      return null;
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  /**
   * 添加key-value及设置过期时间
   *
   * @param key
   * @param value
   * @param expiretime
   * @return
   */
  public static String setObject(String key, Object value, int expiretime) {
    String result = "";
    try {
      if (existsObject(key)) {
        delkeyObject(key);
      }

      result = jedis.set(key.getBytes(), SerializeUtils.serialize(value));
      if (result.equals("OK")) {
        jedis.expire(key.getBytes(), expiretime);
      }
      return result;
    } catch (Exception e) {
      logger.error("setObject设置redis键值异常:key=" + key + " value=" + value + " cause:" + e.getMessage());
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return result;
  }

  /**
   * 删除key
   *
   * @param key
   * @return
   */
  public static Long delkeyObject(String key) {
    try {
      if (RedisUtils.existsObject(key)) {
        return jedis.del(key.getBytes());
      }
      return 0L;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * 判断某个key是否存在
   *
   * @param key
   * @return
   */
  public static Boolean existsObject(String key) {
    return jedis.exists(key.getBytes());
  }

}
