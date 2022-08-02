package com.atguigu.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    //第一步的优化为     habse的读取数据的所在位置,第一次需要访问zookepper得到数据的存放位置信息,接下来的读取数据就可以简化这一步操作的时间
    //第二步的优化为  将需要读取的数据读取之后存储到redis(基于内存的数据库之中) ,在数据发生更新后,删除再重新写入,使用封装redis实现了代码的的读写,删除操作分离,
    //第三步的优化为 数据一起来的时候,同步机制使得排队等待申请,在这种时候可选用增加线程的并行度,或者异步IO来加快数据的处理流程,高并行度对设备的要求高,存在资源的浪费
    public static JSONObject getDimInfo(Jedis jeids, Connection connection, String tableName, String key) throws Exception {
//读取Redis中的维表shuju
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimInfoStr = jeids.get(redisKey);
        if (dimInfoStr != null) {
            //重置过期时间
            jeids.expire(redisKey, 24 * 60 * 60);
            return JSON.parseObject(dimInfoStr);
        }


        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + "where id='" + key + "'";
        //查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfo = queryList.get(0);

        //将从Phoenix查询到的数据写入到Redis
        jeids.set(redisKey, dimInfo.toJSONString());

        //设置过期时间
        jeids.expire(redisKey, 24 * 60 * 60);


        //返回结果

        return dimInfo;

    }

    //用做数据更新后Redis的删除
    public static void delDimInfo(Jedis jedis, String tableName, String key) {
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);
    }


    public static void main(String[] args) throws Exception {

        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        JedisPool jedisPool = JedisPoolUtil.getJedisPool();
        Jedis jedis = jedisPool.getResource();

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(jedis, connection, "DIM_BASE_TRADEMARK", "13"));
        long end = System.currentTimeMillis();
//148 148 143 148 140

        System.out.println(getDimInfo(jedis, connection, "DIM_BASE_TRADEMARK", "13"));
        long end2 = System.currentTimeMillis();
//10 8 8 9   0 1 1 1 1
        System.out.println(end - start);
        System.out.println(end2 - start);

        jedis.close();
        jedisPool.close();
        connection.close();
        dataSource.close();

    }
}
