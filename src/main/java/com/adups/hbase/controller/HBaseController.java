package com.adups.hbase.controller;

import com.adups.hbase.service.HBaseService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * hbase对外api
 * Created by gad on 2018/1/22.
 */
@RestController
public class HBaseController {

	@Autowired
	private HBaseService hBaseService;

	//http://localhost:18080/hbase/command/table/create/
	@RequestMapping(value = "/hbase/command/table/create", method = RequestMethod.GET)
	public String createTable(@RequestParam(value = "tableName") String tableName) throws Exception {
		hBaseService.createTable(tableName, new String[]{"info"},true);
		return "create table success!";
	}

	//http://localhost:18080/hbase/command/table/delete/
	@RequestMapping(value = "/hbase/command/table/delete", method = RequestMethod.GET)
	public String deleteTable(@RequestParam(value = "tableName") String tableName) throws Exception {
		hBaseService.deleteTable(tableName);
		return "delete table success!";
	}

	//http://localhost:18080/hbase/command/scan?tableName="gemini.t_order"
	@RequestMapping(value = "/hbase/command/scan", method = RequestMethod.GET)
	public String scanRegexRowKey(@RequestParam(value = "tableName") String tableName) {
		String regexKey = "000028d224c6474da412ee951d0db906";
		Result result = hBaseService.getRow(tableName, regexKey);
		if (null==result) {
			System.out.println("result is null");
			return "scan data is empty by the rowkey";
		}
		for (Cell cell : result.listCells()) {
			System.out.println("rowKey:" + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
			System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			System.out.println("Timestamp:" + cell.getTimestamp());
		}
		return "scan success";
	}



}
