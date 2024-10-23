package com.atguigu.edu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author yhm
 * @create 2023-04-28 20:51
 */
public class DimUtil {
    /**
     * 如果不填写主键关联的列名  默认是id
     * @param tableName
     * @param id
     * @return
     */
    public static JSONObject getDimInfo(String tableName,String id){
         return getDimInfo(tableName,Tuple2.of("ID",id));
    }

    /**
     * 使用redis进行旁路缓存
     * @param tableName
     * @param columnNamesAndValues
     * @return
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String,String>... columnNamesAndValues){
        JSONObject result = null;
        StringBuilder sql = new StringBuilder("select * from " + EduConfig.HBASE_SCHEMA + "." + tableName + " where ");
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        // 把属性名和属性值的过滤条件都添加进sql中
        for (int i = 0; i < columnNamesAndValues.length; i++) {
            Tuple2<String, String> columnNamesAndValue = columnNamesAndValues[i];
            String columnName = columnNamesAndValue.f0;
            String columnValue = columnNamesAndValue.f1;
            sql.append(columnName + " = " + "'" +columnValue+ "'");
            redisKey.append(columnName + ":" + columnValue);
            if (i <  columnNamesAndValues.length - 1){
                sql.append(" and ");
                redisKey.append("_");
            }
        }
        System.out.println("phoenix查询sql语句如下" + sql.toString());
        Jedis jedis = null;
        String dimJsonStr = null;
        try {
            jedis=JedisUtil.getJedis();
            dimJsonStr = jedis.get(redisKey.toString());
        }catch (Exception e){
            e.printStackTrace();
        }

        // 判断缓存是否命中
        if (dimJsonStr != null && dimJsonStr.length()>0){
            // 缓存命中直接返回
            result = JSON.parseObject(dimJsonStr);
        }else {
            List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql.toString(), JSONObject.class);

            if (jsonObjects != null && jsonObjects.size()>0){
                result = jsonObjects.get(0);
                // 将从Phoenix中读取的数据写入缓存中
                if (jedis != null){
                    jedis.setex(redisKey.toString(),3600 * 24,result.toJSONString());
                }
            }else{
                System.out.println("dim层当前没有对应的维度数据");
            }
        }
        // 释放资源
        if (jedis !=null){
            jedis.close();
        }



        return result;

    }

    public static void deleteCached(String tableName,String id){
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        redisKey.append( "ID:" + id);
        Jedis jedis = null;
        try {
            jedis = JedisUtil.getJedis();
            jedis.del(redisKey.toString());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("清除redis缓存错误");
        }finally {
            if (jedis !=null){
                jedis.close();
            }
        }
    }

    /**
     * 不使用redis进行旁路缓存
     * @param tableName
     * @param columnNamesAndValues
     * @return
     */
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String,String>... columnNamesAndValues){
        StringBuilder sql = new StringBuilder("select * from " + EduConfig.HBASE_SCHEMA + "." + tableName + " where ");
        // 把属性名和属性值的过滤条件都添加进sql中
        for (int i = 0; i < columnNamesAndValues.length; i++) {
            Tuple2<String, String> columnNamesAndValue = columnNamesAndValues[i];
            String columnName = columnNamesAndValue.f0;
            String columnValue = columnNamesAndValue.f1;
            sql.append(columnName + " = " + "'" +columnValue+ "'");
            if (i <  columnNamesAndValues.length - 1){
                sql.append(" and ");
            }
        }
        System.out.println("phoenix查询sql语句如下" + sql.toString());

        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
        JSONObject result = null;
        if (jsonObjects != null && jsonObjects.size()>0){
            result = jsonObjects.get(0);
        }else{
            System.out.println("dim层当前没有对应的维度数据");
        }
        return result;

    }
}
