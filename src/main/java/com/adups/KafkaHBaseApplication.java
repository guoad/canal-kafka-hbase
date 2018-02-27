package com.adups;


import com.adups.canal.CanalClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * 程序入口
 * Created by gad on 2018/2/23.
 */
@SpringBootApplication
@EnableAsync
public class KafkaHBaseApplication {

	protected final static Logger logger = LoggerFactory.getLogger(KafkaHBaseApplication.class);

	@Bean
	CanalClient canalClient() {
		return new CanalClient();
	}

	public static void main(String[] args) throws Exception {

		ApplicationContext context = SpringApplication.run(KafkaHBaseApplication.class, args);

		CanalClient canalClient = context.getBean(CanalClient.class);
		canalClient.start();
		logger.info("## started the canal client ##");

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				try {
					logger.info("## stop  the canal client ##");
					canalClient.stop();
				} catch (Throwable e) {
					logger.warn("##something goes wrong when stopping canal:##", e);
				} finally {
					logger.info("## canal client is down ##");
				}
			}

		});

	}
}
