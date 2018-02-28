package com.adups.hbase.service;

import com.adups.hbase.dao.HBaseDao;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * HBaseService Mutator 实现类
 * Created by gad on 2018/2/22.
 */
@Service
public class HBaseServiceImpl implements HBaseService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);

    @Autowired
    private HBaseDao hBaseDao;

    /**
     * 创建表
     * @param tableName         表名称
     * @param columnFamilies    列族名称数组
     * @param preBuildRegion    是否预分配Region   true:是;   false:否   默认16个region，rowkey生成的时候记得指定前缀
     * @return                  返回执行时间 (单位: 毫秒)
     */
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        hBaseDao.createTable(tableName, columnFamilies, preBuildRegion);
    }

    @Override
    public void deleteTable(String tableName) {
        try {
            hBaseDao.deleteTable(tableName);
        } catch (IOException e) {
            logger.error("HBase delete table failed.", e);
        }
    }

    /**
     * 单行单个列族存储数据
     * @param tableName         表名称
     * @param rowKey            rowkey
     * @param familyColumn      列族名称
     * @param columnValues      列值集合
     * @param waiting           是否等待线程执行完成  true:可以及时看到结果;  false:让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    @Override
    public void put(String tableName,  String rowKey, String familyColumn, Map<String, String> columnValues, boolean waiting) {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> entry : columnValues.entrySet()) {
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        batchPut(tableName, Arrays.asList(put), waiting);
     }

    /**
     * 获取多行数据
     * @param tablename   表名称
     * @param rows        多行
     * @return
     */
    @Override
    public <T> Result[] getRows(String tablename, List<T> rows) {
        return hBaseDao.getRows(tablename, rows);
    }

    /**
     * 获取单行数据
     * @param tablename   表名称
     * @param rowKey      rowKey
     * @return
     */
    @Override
    public Result getRow(String tablename, String rowKey) {
        //查询hbase，getRow()异步线程任务。
        Future<Result> f = hBaseDao.getRow(tablename, rowKey.getBytes());
        Result rs = null;
        try {
            //因为是查询操作，需要立即返回数据，故此紧接着f.get()；
            rs = f.get();
        } catch (InterruptedException e) {
            logger.error("HBase get row data failed.", e);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 删除rowKey那行数据
     * @param tableName
     * @param rowKey
     */
    @Override
    public void deleteAllColumn(String tableName, String rowKey) {
        hBaseDao.delete(tableName, rowKey);
    }

    /**
     * 多线程同步存储数据
     * @param tableName  表名称
     * @param puts       待提交参数
     * @param waiting    是否等待线程执行完成  true:可以及时看到结果;  false:让线程继续执行，并跳出此方法返回调用方主程序
     */
    @Override
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future<Long> f = null;
        try {
            f = hBaseDao.put(tableName, puts);
        } catch (Exception e) {
            logger.error("batchPut failed.", e);
        }
        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out.", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 多线程异步存储数据
     * @param tableName  表名称
     * @param puts       待提交参数
     * @param waiting    是否等待线程执行完成  true:可以及时看到结果;   false:让线程继续执行，并跳出此方法返回调用方主程序
     */
    public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future<Long> f = null;
        try {
            f = hBaseDao.putByHTable(tableName, puts);
        } catch (Exception e) {
            logger.error("batchPut failed . ", e);
        }
        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out.", e);
            } catch (ExecutionException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }



}
