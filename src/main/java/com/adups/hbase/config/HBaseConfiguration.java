package com.adups.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

/**
 * hbase加载zookeeper配置信息
 * Created by gad on 2018/1/16.
 */
@org.springframework.context.annotation.Configuration
public class HBaseConfiguration {

	private  Logger logger= LoggerFactory.getLogger(HBaseConfiguration.class);

	@Value("${hbase.zookeeper.quorum}")
	private String quorum;

	/**
	 * 产生HBaseConfiguration实例化Bean
	 * @return
	 */
	@Bean
	public Configuration configuration() {
		Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",quorum);
		conf.set("zookeeper.znode.parent", "/hbase");
		return conf;
	}
}
