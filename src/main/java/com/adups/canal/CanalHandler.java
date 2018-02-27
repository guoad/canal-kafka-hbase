package com.adups.canal;

import com.adups.kafka.producer.HandlerProducer;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.List;
import java.util.Map;

/**
 * Created by gad on 2018/2/23.
 */
@Component
public class CanalHandler {

    protected final static Logger logger = LoggerFactory.getLogger(CanalHandler.class);

    @Autowired
    private HandlerProducer handlerProducer;

    /**
     * 处理canal发送过来的数据
     * @param running    是否继续
     * @param connector  连接canal服务器的连接对象
     */
    protected void handler(boolean running, CanalConnector connector) {
        try {
            int batchSize = 5 * 1024;
            connector.connect();
            connector.subscribe();
            while (running) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    //logger.info("accept canal data of emtry is empty!");
                } else {
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
            }
        } catch (Exception e) {
            logger.error("accept canal data but handle error!", e);
        } finally {
            connector.disconnect();
        }

    }

    protected void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id
                    logger.info("BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("END ----> transaction id: {}", end.getTransactionId());
                }
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChage.getEventType();
                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info("QuerySql|DdlSql ----> " + rowChage.getSql());
                    CanalBean canalBean = new CanalBean(entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),
                            entry.getHeader().getExecuteTime(),eventType.getNumber(),rowChage.getSql());
                    //向kafka发送数据
                    handlerProducer.sendMessage(canalBean,true);
                    continue;
                }
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    CanalBean canalBean = new CanalBean(entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),
                            entry.getHeader().getExecuteTime(),eventType.getNumber(),null);
                    Map<String, CanalBean.RowData.ColumnEntry> beforeColumns = printColumnToList(rowData.getBeforeColumnsList());
                    Map<String, CanalBean.RowData.ColumnEntry> afterColumns = printColumnToList(rowData.getAfterColumnsList());
                    canalBean.setRowData(new CanalBean.RowData(beforeColumns,afterColumns));
                    //向kafka发送数据
                    handlerProducer.sendMessage(canalBean,true);
                }
            }
        }
    }

    protected Map<String, CanalBean.RowData.ColumnEntry> printColumnToList(List<CanalEntry.Column> columns) {
        Map<String, CanalBean.RowData.ColumnEntry> map = new ConcurrentReferenceHashMap<>();
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append("name:" + column.getName() + " + isKey:" + column.getIsKey() + " + updated:" + column.getUpdated() + " + isNull:" + column.getIsNull() + " + value:" + column.getValue());
            logger.info(builder.toString());
            CanalBean.RowData.ColumnEntry columnEntry = new CanalBean.RowData.ColumnEntry(column.getName(),column.getIsKey(),column.getUpdated(),column.getIsNull(),column.getValue());
            map.put(column.getName(), columnEntry);
        }
        return map;
    }


}
