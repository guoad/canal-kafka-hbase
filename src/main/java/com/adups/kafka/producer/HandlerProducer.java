package com.adups.kafka.producer;

import com.adups.canal.CanalBean;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * kafka的producer
 * Created by gad on 2018/1/18.
 */
@Component
public class HandlerProducer {

    private Logger logger = LoggerFactory.getLogger(HandlerProducer.class);

    @Autowired
    private KafkaProducerTask kafkaProducerTask;

    /**
     * 多线程同步提交
     * @param canalBean canal传递过来的bean
     * @param waiting   是否等待线程执行完成 true:可以及时看到结果; false:让线程继续执行，并跳出此方法返回调用方主程序;
     */
    public void sendMessage(CanalBean canalBean, boolean waiting) {
        String canalBeanJsonStr = JSON.toJSONString(canalBean);
        Future<String> f = kafkaProducerTask.sendKafkaMessage(canalBean.getDatabase() + "." + canalBean.getTable(),canalBeanJsonStr);
        logger.info("HandlerProducer日志--->当前线程:" + Thread.currentThread().getName() + ",接受的canalBeanJsonStr:" + canalBeanJsonStr);
        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("Send kafka message job thread pool await termination time out.", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }


}
