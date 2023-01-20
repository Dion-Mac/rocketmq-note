package org.apache.rocketmq.example.schedule;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class MyScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        // 实例化一个生产者来产生延时消息
        // 比如电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。
        DefaultMQProducer producer = new DefaultMQProducer("MyScheduledMessageProducer");
        producer.setNamesrvAddr("182.61.6.159:9876");
        producer.start();
        int totalMessageSend = 100;
        for (int i = 0; i < totalMessageSend; i++) {
            Message message = new Message("TestScheduledMessage", ("Hello scheduled message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //设置延时等级为3，这个消息将在10s之后发送(立即发送给broker, consumer在10s后消费)（现在只支持固定的几个时间,详细看delayTimeLevel）
            message.setDelayTimeLevel(3);
            LocalDateTime now = LocalDateTime.now();
            System.out.println("当前发送消息的时间是: " + now.getYear() + "年" + now.getMonthValue() + "月" + now.getDayOfMonth() + "日" +
                    now.getHour() + "时" + now.getMinute() + "分钟" + now.getSecond() + "秒");
            //发送消息
            SendResult sendResult = producer.send(message);
            System.out.println("消息状态：" + sendResult.getSendStatus());
            System.out.println("消息id：" + sendResult.getMsgId());
            System.out.println("消息queue：" + sendResult.getMessageQueue());
            System.out.println("消息offset：" + sendResult.getQueueOffset());
        }
        producer.shutdown();
    }
}
