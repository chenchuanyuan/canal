package com.alibaba.otter.canal.client.running.kafka;

import org.junit.Assert;

/**
 * Kafka 测试基类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public abstract class AbstractKafkaTest {

    public static String  topic     = "canaltest_person,canaltest_comments";
    public static Integer partition = null;
    public static String  groupId   = "g41224122";
    public static String  servers   = "erp2.test.pagoda.com.cn:9092";
    public static String  zkServers = "erp2.test.pagoda.com.cn:2083";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}
