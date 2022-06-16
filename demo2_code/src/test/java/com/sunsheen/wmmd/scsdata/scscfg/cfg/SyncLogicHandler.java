package com.sunsheen.wmmd.scsdata.scscfg.cfg;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @ClassName: SyncLogicHandler
 * @ToDo: (用于处理同步相关的逻辑)
 * @Desc: (具体功能描述)
 * @author: SLM
 * @date 2022年3月24日 下午5:12:24
 * @Copyright: 2022 www.sunsheen.com Inc. All rights reserved.
 */
public class SyncLogicHandler {

    /**
     * 线程池数量
     */
    private static final int THREAD_POOL = 4;

    private static final String DELETE = "delete";
    private static final String INSERT = "insert";
    private static final String UPDATE = "update";

    /**
     * 批量提交的数量
     */
    private static final int LIMIT = 1_0000;

    /**
     * 累加获取条数,计算存储的数据量
     */
    private AtomicLong currentSynCount = new AtomicLong(0L);

    /**
     * 容器
     */
    private CopyOnWriteArrayList<Map<String, Object>> updataCollections = new CopyOnWriteArrayList<Map<String, Object>>();
    private CopyOnWriteArrayList<Map<String, Object>> insertCollections = new CopyOnWriteArrayList<Map<String, Object>>();
    private CopyOnWriteArrayList<Map<String, Object>> deleteCollections = new CopyOnWriteArrayList<Map<String, Object>>();


    // 表名称
    private String tableName = "tab_omin_cm_cc_coremeta_stationps";
    // 主键名
    private String primaryKey = "C_APPLY_ID";
    // 表字段集合
    private String[] fieldColumns;
    // 限制批量查询的最大量
    private static final int MAX_QUERY = 1_000;
    // api总数据量
    private int API_DATA_NUM = 0;
    // 匹配标记key,目前无法做到已经匹配到的数据进行移除
    private static final String flag_key = "this_my_flag";

    SyncLogicHandler() throws Exception {
        this.fieldColumns = queryTableColumnField();
    }


    public long getCurrentSynCount() {
        return currentSynCount.get();
    }


    /**
     * @ToDo: (通过线程去执行操作)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年4月12日 下午5:14:09
     */
    private boolean executeDataByThread(List<Map<String, Object>> reqCollections, List<Map<String, Object>> dbCollections) throws Exception {
        boolean flag = true;
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL);
        int batch = (API_DATA_NUM > LIMIT) ? ((int) Math.ceil((API_DATA_NUM / LIMIT))) : 1;
        try {
            multiThreadedBatchProcess(executor, 1, reqCollections, dbCollections);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            executor.shutdown();
            while (true) {
                if (executor.isTerminated()) {
                    System.out.println("所有的子线程都结束了！");
                    break;
                }
                Thread.sleep(1000);
            }


        }
        return flag;
    }

    /**
     * @ToDo: (处理多批次的情况)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月30日 上午10:57:48
     */
    private void multiThreadedBatchProcess(ExecutorService executor, int batch, List<Map<String, Object>> collections, List<Map<String, Object>> dbCollections) throws Exception {
        try {
            System.out.printf("collections:%d条，dbCollections:%d条\n", collections.size(), dbCollections.size());
            WorkerHandler workerHandler = new WorkerHandler(collections, dbCollections);
            executor.execute(workerHandler);
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * @ClassName: WorkerHandler
     * @ToDo: (插入数据库的多线程)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月24日 下午5:21:01
     * @Copyright: 2022 www.sunsheen.com Inc. All rights reserved.
     */
    final class WorkerHandler implements Runnable {

        private List<Map<String, Object>> originalCollections;
        private Map<String, Map<String, Object>> dbCollections;


        WorkerHandler(List<Map<String, Object>> originalCollections, List<Map<String, Object>> dbCollections) {
            this.originalCollections = originalCollections;
            this.dbCollections = groupByKey(dbCollections);
        }


        @Override
        public void run() {
            try {
                compareCollections(originalCollections, dbCollections);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    //根据主键分组
    private Map<String, Map<String, Object>> groupByKey(List<Map<String, Object>> collections) {
        Map<String, Map<String, Object>> mapp = new ConcurrentHashMap<>();
        for (Map<String, Object> map : collections) {
            String keyValue = (String) map.get(primaryKey);
            if (mapp.containsKey(keyValue)) {
                System.out.printf("主键重复：%s\n", keyValue);
            }
            mapp.put(keyValue, map);
        }
        return mapp;
    }


    /**
     * @return
     * @ToDo: (校验数据)
     * @Desc: (分批查询DB的数据 ， 进行颗粒级对比 ， 分成两个容器 ， 一个存在则更新 ， 不存在则插入)
     * @author: SLM
     * @date 2022年3月28日 上午11:25:20
     */
    @SuppressWarnings("unchecked")
    boolean matchingMassiveHandler(List<Map<String, Object>> reqCollections) throws Exception {
        this.API_DATA_NUM = reqCollections.size();

        List<Map<String, Object>> targetCollections = queryTargetTableData(reqCollections);

        executeDataByThread(reqCollections, targetCollections);
        // 保存数据
        saveAllTypeOfProcessedData();

        return true;
    }


    /**
     * @ToDo: (查询目标表全部数据)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年4月12日 下午5:06:39
     */
    private List<Map<String, Object>> queryTargetTableData(List<Map<String, Object>> reqCollections) {
        return reqCollections.subList(0, 7_0000);
    }


    /**
     * @ToDo: (处理不同字段数据格式)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月24日 下午3:07:08
     */
    private Object dataTypeHandler(String key, Object value) {
        // 最后更新时间
        if (key.equals("c_updated_date") || key.equals("c_create_date")) {
            value = parseDateStr(value);
        }

        if (StrUtil.isEmptyIfStr(value)) {
            value = "";
        }
        return value;
    }


    /**
     * @throws Exception
     * @ToDo: (匹配每一个批次的数据)
     * @author: SLM
     * @date 2022年3月28日 上午11:30:58
     */
    private void compareCollections(List<Map<String, Object>> originalCollections, Map<String, Map<String, Object>> dbCollections) throws Exception {
        System.out.printf("originalCollections : %d条数据,dbCollections : %d条数据 \n", originalCollections.size(), dbCollections.size());


        if (dbCollections.size() == 0) {
            insertCollections.addAll(originalCollections);
            return;
        }
        // 复制数据
        for (Map<String, Object> originalMap : originalCollections) {
            // 判断是否是物理删除标识
            if (isDelete(originalMap)) {
                add2Container(originalMap, DELETE);
                continue;
            }

            //新增
            Map<String, Object> dbMap = dbCollections.get(originalMap.get(primaryKey));
            if (dbMap == null) {
                add2Container(originalMap, INSERT);
                continue;
            }


            // 更新
            matchEachField(originalMap, dbMap);

        }
    }

    /**
     * @return
     * @ToDo: (判断是否是删除标识)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月31日 下午2:15:45
     */
    private boolean isDelete(Map<String, Object> map) {
        String value = String.valueOf(map.get("c_opt_type"));
        String value1 = String.valueOf(map.get("C_OPT_TYPE"));
        return (StrUtil.isNotEmpty(value) || StrUtil.isNotEmpty(value1)) && (DELETE.equalsIgnoreCase(value) || DELETE.equalsIgnoreCase(value1));
    }


    /**
     * @return
     * @ToDo: (根据数据库结构信息)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月29日 下午2:15:44
     */
    private Map<String, Object> basedOnDatabaseFieldInfo(Map<String, Object> req) {
        Map<String, Object> map = new ConcurrentHashMap<String, Object>();
        for (String key : fieldColumns) {
            map.put(key, String.valueOf(dataTypeHandler(key, req.get(key))));
        }
        return map;
    }

    /**
     * @ToDo: (对比每一个字段)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月28日 上午11:56:04
     */
    private void matchEachField(Map<String, Object> req, Map<String, Object> db) {
        Map<String, Object> reqMap = basedOnDatabaseFieldInfo(req);
        Map<String, Object> dbMap = basedOnDatabaseFieldInfo(db);
        if (reqMap.equals(dbMap)) {
            return;
        }
        add2Container(req, UPDATE);
    }


    /**
     * @Todo: (格式化成Date)
     * @author: SLM
     * @date 2021年1月18日 上午10:54:32
     */
    private String parseDateStr(Object time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            if (StrUtil.isEmptyIfStr(time)) {
                return DateUtil.now();
            }

            if (time instanceof Date) {
                return sdf.format((Date) time);
            }

            if (time instanceof Calendar) {
                return sdf.format(((Calendar) time).getTime());

            }
            if (time instanceof Long) {
                Date date = new Date();
                date.setTime((Long) time);
                return sdf.format(date);
            }

            if (time instanceof String) {
                String tempTime = String.valueOf(time);
                if (tempTime.length() == 13) {
                    Date date = new Date();
                    date.setTime(Long.valueOf(tempTime));
                    return sdf.format(date);
                }

                if (tempTime.length() > 18) {
                    return tempTime.substring(0, 19);

                }
                if (tempTime.length() == 10) {
                    return tempTime + " 00:00:00";
                }
                if (tempTime.length() == 16) {
                    return tempTime + ":00";
                }
            }

        } catch (Exception e) {
            System.out.println("*********未知时间类型转换[" + time + "]异常*************");
        }
        return null;
    }


    /**
     * @ToDo: (这个功能是干嘛的 ?)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月31日 下午4:00:58
     */
    private void add2Container(Map<String, Object> map, String type) {
        if (DELETE.equals(type)) {
            deleteCollections.add(map);

        } else if (INSERT.equals(type)) {
            insertCollections.add(map);

        } else if (UPDATE.equals(type)) {
            updataCollections.add(map);
        }
    }

    /**
     * @ToDo: (保存数据)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年4月12日 下午5:12:09
     */
    private void saveAllTypeOfProcessedData() throws Exception {
        System.out.printf("insertCollections.size() = %s \n", insertCollections.size());
        System.out.printf("updateCollections.size() = %s \n", updataCollections.size());
        System.out.printf("deleteCollections.size() = %s \n", deleteCollections.size());

        currentSynCount.addAndGet(insertCollections.size());
        currentSynCount.addAndGet(updataCollections.size());
        currentSynCount.addAndGet(deleteCollections.size());
    }

    /**
     * @return
     * @ToDo: (获取字段信息)
     * @Desc: (具体功能描述)
     * @author: SLM
     * @date 2022年3月24日 下午2:54:39
     */
    private String[] queryTableColumnField() {
        String[] fields = {"c_coremeta_station_id", "c_coremeta_id", "c_siteopf_id", "c_tempele_id", "c_pri", "c_netstation_type", "c_status", "c_create_date", "c_updated_date", "c_formal"};
        fields[fields.length - 1] = primaryKey;
        return fields;
    }
}
