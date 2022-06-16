package com.sunsheen.wmmd.scsdata.scscfg.cfg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class SerializeUtils {


  /**
   * 序列化
   *
   * @param object
   * @return
   */
  public static byte[] serialize(Object object) {
    ObjectOutputStream objectOutputStream = null;
    ByteArrayOutputStream byteArrayOutputStream = null;
    try {
      // 序列化
      byteArrayOutputStream = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(object);
      byte[] bytes = byteArrayOutputStream.toByteArray();
      return bytes;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 反序列化
   *
   * @param bytes
   * @return
   */
  public static Object unserialize(byte[] bytes) {
    ByteArrayInputStream byteArrayInputStream = null;
    try {
      // 反序列化
      byteArrayInputStream = new ByteArrayInputStream(bytes);
      ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
      return objectInputStream.readObject();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
