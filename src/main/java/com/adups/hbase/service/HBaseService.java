package com.adups.hbase.service;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.Map;

/**
 * HBase 服务接口类
 * Created by gad on 2018/2/22.
 */
public interface HBaseService {

    /**
     * 创建表
     * @param tableName         表名称
     * @param columnFamilies    列族名称数组
     * @param preBuildRegion    是否预分配Region   true:是;   false:否  默认16个region，rowkey生成的时候记得指定前缀
     * @return
     */
    void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception;

    /**
     * 创建表
     * @param tableName         表名称
     * @return
     */
    void deleteTable(String tableName);

    /**
     * 单行单个列族存储数据
     * @param tableName         表名称
     * @param rowKey            rowkey
     * @param familyColumn      列族名称
     * @param columnValues      列值集合
     * @param waiting           是否等待线程执行完成  true:可以及时看到结果;  false:让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    void put(String tableName, String rowKey, String familyColumn, Map<String, String> columnValues, boolean waiting);

    /**
     * 批量写入数据
     * @param tableName  表名称
     * @param puts       Put 类型的列表
     * @param waiting    是否等待线程执行完成  true:可以及时看到结果;  false:让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    void batchPut(String tableName, final List<Put> puts, boolean waiting);

    /**
     * 获取多行数据
     * @param tablename   表名称
     * @param rows        多行
     * @return
     */
    <T> Result[] getRows(String tablename, List<T> rows);

    /**
     * 获取单行数据
     * @param tablename   表名称
     * @param rowKey      rowKey
     * @return
     */
    Result getRow(String tablename, String rowKey);

    /**
     * 删除rowKey那行数据
     * @param tableName
     * @param rowKey
     */
    void deleteAllColumn(String tableName,String rowKey);
}
