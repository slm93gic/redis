package com.sunsheen.wmmd.scsdata.scscfg.cfg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

public class Test {

    public static void main(String[] args) throws Exception {
        Long start = System.currentTimeMillis();
        SyncLogicHandler handler = new SyncLogicHandler();
        handler.matchingMassiveHandler(getData());
        System.out.printf("合计：%s \n", handler.getCurrentSynCount());
        System.out.printf("合计：%s \n", 169716 - 70000);
        Long end = System.currentTimeMillis();
        System.out.println("耗时：" + (end - start) / 1000 + "秒");
    }


    private static List<Map<String, Object>> getData() {
        String data = (String) RedisUtils.getObject("tab_omin_cm_cc_coremeta_stationps_org");
        JSONObject myJson = JSON.parseObject(data);
        // ====================接口访问成功====================
        return (List<Map<String, Object>>) myJson.get("data");

    }


}
