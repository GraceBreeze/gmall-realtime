package com.atguigu.gmall.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.GmallConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author yhm
 * @create 2022-08-27 15:31
 */
public class DimUtil {
    // select * from t where id = 'v.id' and user_id = '10'
    public static JSONObject getDimInfo(Connection connection,String tableName,String id){

        // 优先读取redis
        Jedis jedis = null;

        String redisKey = "DIM:" + tableName + ":" + id;
        String value = null;
        try {
            jedis = JedisUtil.getJedis();
            value = jedis.get(redisKey);
        }catch (Exception e){
            System.out.println("jedis异常");
        }

        JSONObject result = null;
        if (value != null){
            // 如果redis中有存储的数据  直接返回
            result = JSON.parseObject(value);
        }else {
            // 如果redis中没有存储的数据  查询phoenix
            List<JSONObject> dimInfos = getDimInfo(connection, tableName, new Tuple2<>("ID", id));
            if (dimInfos.size() > 0){
                result = dimInfos.get(0);
                // 写入到redis中
                try {
                    jedis.setex(redisKey,3600 * 24,result.toJSONString());
                }catch (Exception e){
                    System.out.println("jedis异常");
                }
            }
        }
        if (jedis !=null){
            jedis.close();
        }
        return result;
    }

    public static List<JSONObject> getDimInfo(Connection connection, String tableName, Tuple2<String,String>... tuple2s){
        StringBuilder sql = new StringBuilder();
        sql.append("select * from ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append(" where ");
        for (int i = 0; i < tuple2s.length; i++) {
            sql.append(tuple2s[i].f0)
                    .append(" = '")
                    .append(tuple2s[i].f1)
                    .append("'");
            if (i < tuple2s.length - 1){
                sql.append(" and ");
            }
        }
        System.out.println(sql.toString());

        return PhoenixUtil.sqlQuery(connection, sql.toString(), JSONObject.class);
    }


    public static void deleteRedisCache(String tableName,String id){
        String redisKey = "DIM:" +tableName + ":" + id;
        try{
            Jedis jedis = JedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        }catch (Exception e){
            System.out.println("jedis连接异常");
        }
    }
}
