package org.apache.rocketmq.example.template.producer;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;

/**
 * 延迟消息发送
 * 3.4 延时消息的使用场景
 * 比如电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。
 *
 * 3.5 延时消息的使用限制
 * // org/apache/rocketmq/store/config/MessageStoreConfig.java
 *
 * private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
 * 现在RocketMq并不支持任意时间的延时，需要设置几个固定的延时等级，从1s到2h分别对应着等级1到18 消息消费失败会进入延时消息队列，消息发送时间与设置的延时等级和重试次数有关，详见代码SendMessageProcessor.java
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();
        int totalMessagesToSend = 10;
        for (int i = 0; i < totalMessagesToSend; i++) {
            String format = DateFormatUtils.format(new Date(), "yyyy-MM-dd'T'HH:mm:ss.SSS");
            String msg = "Hello scheduled message " + i + ",time:" + format;
            Message message = new Message("TestTopic", msg.getBytes());
            // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(3);
            // 发送消息
            producer.send(message);
            System.out.println("send msg:"+msg);
        }
        // 关闭生产者
        producer.shutdown();
    }
}