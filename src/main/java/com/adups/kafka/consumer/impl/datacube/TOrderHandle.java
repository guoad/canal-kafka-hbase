package com.adups.kafka.consumer.impl.datacube;

import com.adups.canal.CanalBean;
import com.adups.hbase.config.BaseConfig;
import com.adups.hbase.service.HBaseService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 业务t_order的处理
 * Created by gad on 2018/1/22.
 */
@Component
public class TOrderHandle {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private final static String T_STORE = "t_store";
	private final static String T_ESHOP = "t_eshop";
	private final static String T_CUSTOMER = "t_customer";
	private final static String T_ORDER_GROUP = "t_order_group";

	@Autowired
	HBaseService hBaseService;

	public void insert(CanalBean canalBean) {
		logger.info("TOrderHandle--->datacube.t_order～～～～～～insert");
		Map<String, String> columnValues = new HashMap<>(46);
		Map<String, CanalBean.RowData.ColumnEntry> insertColumnEntrys = canalBean.getRowData().getAfterColumns();
		String rowkey  = insertColumnEntrys.get("id").getValue();
		//假定gemini的t_order和datacube的t_order字段平移，其他扩展，如下！
		for(String key:insertColumnEntrys.keySet()){
			if(!key.equals("id")){
				columnValues.put(key,insertColumnEntrys.get(key).getValue());
			}
		}

		boolean flag = false;
		// 1.store_id扩展城市信息；
		String store_id  = insertColumnEntrys.get("store_id").getValue();
		Result result0 = hBaseService.getRow(BaseConfig.TABLE_PREFIX_GEMINI + T_STORE,store_id);
		for (Cell cell : result0.listCells()) {
			String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			if (columnName.equals("white")) {
				String white = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				//store_id和 eshop_id确定white是否=QA测试，是，则不做insert处理。
				if(white.equals("QA")){
					flag = true;
					return;
				}else {
					columnValues.put("store_white",white);
				}
			}
			if("name".equals(columnName)){
				String name = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("store_name",name);
			}
			if("province_code".equals(columnName)){
				String code = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("store_province_code",code);
			}
			//TODO

		}
        // 2.eshop_id扩展门店信息；
		String eshop_id  = insertColumnEntrys.get("eshop_id").getValue();
		Result result1 = hBaseService.getRow(BaseConfig.TABLE_PREFIX_GEMINI + T_ESHOP,eshop_id);
		for (Cell cell : result0.listCells()) {
			String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			if (columnName.equals("white")) {
				String white = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				//store_id和 eshop_id确定white是否=QA测试，是，则不做insert处理。
				if(white.equals("QA")){
					flag = true;
					return;
				}else {
					columnValues.put("eshop_white",white);
				}
			}
			if(columnName.equals("name")){
				String name = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("eshop_name",name);
			}
			if(columnName.equals("seller_id")){
				String seller_id = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("eshopseller_id",seller_id);
			}
			//TODO

		}

		if(flag){
			return;
		}

		// 3.customer_id扩展用户信息；
		String customer_id = insertColumnEntrys.get("customer_id").getValue();
		Result result2 = hBaseService.getRow(BaseConfig.TABLE_PREFIX_GEMINI + T_CUSTOMER,customer_id);
		for (Cell cell : result2.listCells()) {
			String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			if(columnName.equals("mobilephone")){
				String mobilephone = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("customer_mobilephone",mobilephone);
			}
			if(columnName.equals("customer_source")){
				String customer_source = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				columnValues.put("customer_source",customer_source);
			}
			//TODO

		}

		// 4:is_split是否拆单，是，则group_id去order_group查拆单数量，payPrice和trantprice／拆单数量；
		int order_quantity = this.getOrderQuantity(insertColumnEntrys);
		BigDecimal trading_price = BigDecimal.valueOf(Double.parseDouble(insertColumnEntrys.get("trading_price").getValue()));
		BigDecimal payable_price = BigDecimal.valueOf(Double.parseDouble(insertColumnEntrys.get("payable_price").getValue()));
		columnValues.put("trading_price",trading_price.divide(new BigDecimal(order_quantity)).toString());
		columnValues.put("payable_price",payable_price.divide(new BigDecimal(order_quantity)).toString());

		hBaseService.put(BaseConfig.TABLE_PREFIX_DATACUBE+canalBean.getTable(), rowkey, BaseConfig.FAMILY_COLUMN, columnValues,false);
	}

	public void update(CanalBean canalBean) {
		logger.info("TOrderHandle--->datacube.t_order～～～～～～update");
		Map<String, String> columnValues = new HashMap<>(46);
		Map<String,CanalBean.RowData.ColumnEntry> updateColumnEntrys = canalBean.getRowData().getAfterColumns();
		String rowkey  = updateColumnEntrys.get("id").getValue();
		for(String key:updateColumnEntrys.keySet()){
			if((!key.equals("id"))&&updateColumnEntrys.get(key).getUpdated()){
				columnValues.put(key,updateColumnEntrys.get(key).getValue());
			}
		}
		if(updateColumnEntrys.get("update_time").getUpdated()){
			columnValues.put("order_signed_time",updateColumnEntrys.get("update_time").getValue());
		}
		int order_quantity = this.getOrderQuantity(updateColumnEntrys);
		if(updateColumnEntrys.get("trading_price").getUpdated()){
			BigDecimal trading_price = BigDecimal.valueOf(Double.parseDouble(updateColumnEntrys.get("trading_price").getValue()));
			columnValues.put("trading_price",trading_price.divide(new BigDecimal(order_quantity)).toString());
		}
		if(updateColumnEntrys.get("payable_price").getUpdated()){
			BigDecimal payable_price = BigDecimal.valueOf(Double.parseDouble(updateColumnEntrys.get("payable_price").getValue()));
			columnValues.put("payable_price",payable_price.divide(new BigDecimal(order_quantity)).toString());
		}

		hBaseService.put(BaseConfig.TABLE_PREFIX_DATACUBE+canalBean.getTable(), rowkey, BaseConfig.FAMILY_COLUMN, columnValues,false);
	}

	public void delete(CanalBean canalBean){
		logger.info("TOrderHandle--->datacube.t_order～～～～～～delete");
		Map<String,CanalBean.RowData.ColumnEntry> deleteColumnEntrys = canalBean.getRowData().getBeforeColumns();
		String rowkey  = deleteColumnEntrys.get("id").getValue();
		hBaseService.deleteAllColumn(BaseConfig.TABLE_PREFIX_DATACUBE+canalBean.getTable(), rowkey);
	}

	private int getOrderQuantity(Map<String, CanalBean.RowData.ColumnEntry> mapColumnEntrys){
		// 4:is_split是否拆单，是，则group_id去order_group查拆单数量，payPrice和trantprice／拆单数量；
		String is_split = mapColumnEntrys.get("is_split").getValue();
		int order_quantity = 1;
		if(is_split.equals("yes")) {
			String group_id = mapColumnEntrys.get("group_id").getValue();
			Result result3 = hBaseService.getRow(BaseConfig.TABLE_PREFIX_GEMINI + T_ORDER_GROUP, group_id);
			for (Cell cell : result3.listCells()) {
				String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
				if ("order_quantity".equals(columnName)) {
					order_quantity = Integer.parseInt(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
					return order_quantity;
				}
			}
		}
		return order_quantity;
	}


}
