package com.adups.kafka.consumer.impl.gemini;

import com.adups.canal.CanalBean;
import com.adups.hbase.config.BaseConfig;
import com.adups.hbase.service.HBaseService;
import com.adups.kafka.consumer.impl.datacube.TOrderHandle;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 原始t_order的处理
 * Created by gad on 2018/1/22.
 */
@Component
public class TOrderConsumer {

	private Logger logger = LoggerFactory.getLogger(TOrderConsumer.class);

	@Autowired
	private HBaseService hBaseService;
	@Autowired
	TOrderHandle tOrderHandle;

	@KafkaListener(topics = {"gemini.t_order"})
	public void listen(ConsumerRecord<?, ?> record) {
		logger.info("TOrderConsumer--->gemini.t_order～～～～～～listen");
		logger.info("---|offset = %d,topic= %s,partition=%s,key =%s,value=%s\n", record.offset(), record.topic(), record.partition(), record.key(), record.value());
		logger.info("sss---------:"+record.value().toString());
		CanalBean canalBean = JSON.parseObject(record.value().toString(), CanalBean.class);

		switch(canalBean.getEventType()){
			case CanalEntry.EventType.INSERT_VALUE:
				logger.info("canalBean.getEventType():"+canalBean.getEventType());
				logger.info("CanalEntry.EventType.INSERT_VALUE:"+CanalEntry.EventType.INSERT_VALUE);
				//20服务器上的gemini库的t_order平行同步insert的数据到Hbase的表gemini.t_order
				insert(canalBean);
				//20服务器上的gemini库的t_order扩展同步insert的数据到Hbase的表datacube.t_order
				tOrderHandle.insert(canalBean);
				break;
			case CanalEntry.EventType.UPDATE_VALUE:
				update(canalBean);
				tOrderHandle.update(canalBean);
				break;
			case CanalEntry.EventType.DELETE_VALUE:
				delete(canalBean);
				tOrderHandle.delete(canalBean);
				break;
			case CanalEntry.EventType.CREATE_VALUE:
				//TODO
				break;
			case CanalEntry.EventType.ALTER_VALUE:
				//TODO
				break;
			case CanalEntry.EventType.ERASE_VALUE:
				//TODO
				break;
			case CanalEntry.EventType.QUERY_VALUE:
				//TODO
				break;

		}

	}

	public void insert(CanalBean canalBean) {
		Map<String, String> columnValues = new HashMap<>(46);
		Map<String, CanalBean.RowData.ColumnEntry> insertColumnEntrys = canalBean.getRowData().getAfterColumns();
		String rowkey  = insertColumnEntrys.get("id").getValue();
		for(String key:insertColumnEntrys.keySet()){
			if(!key.equals("id")){
				columnValues.put(key,insertColumnEntrys.get(key).getValue());
			}
		}
		hBaseService.put(BaseConfig.TABLE_PREFIX_GEMINI+canalBean.getTable(), rowkey, BaseConfig.FAMILY_COLUMN, columnValues,false);
	}

	public void update(CanalBean canalBean) {
		Map<String, String> columnValues = new HashMap<>(46);
		Map<String,CanalBean.RowData.ColumnEntry> updateColumnEntrys = canalBean.getRowData().getAfterColumns();
		String rowkey  = updateColumnEntrys.get("id").getValue();
		for(String key:updateColumnEntrys.keySet()){
			if((!key.equals("id"))&&updateColumnEntrys.get(key).getUpdated()){
				columnValues.put(key,updateColumnEntrys.get(key).getValue());
			}
		}
		hBaseService.put(BaseConfig.TABLE_PREFIX_GEMINI+canalBean.getTable(), rowkey, BaseConfig.FAMILY_COLUMN, columnValues,false);
	}

	public void delete(CanalBean canalBean) {
		Map<String, CanalBean.RowData.ColumnEntry> deleteColumnEntrys = canalBean.getRowData().getAfterColumns();
		String rowkey  = deleteColumnEntrys.get("id").getValue();
		hBaseService.deleteAllColumn(BaseConfig.TABLE_PREFIX_GEMINI+canalBean.getTable(), rowkey);
	}


}
