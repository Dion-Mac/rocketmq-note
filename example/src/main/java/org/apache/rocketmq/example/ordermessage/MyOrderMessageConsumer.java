package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MyOrderMessageConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("MyOrderMessageConsumer");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        /**
         * 设置consumer第一次启动的时候是从队列头部开始消费还是队列尾部开始消费
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("OrderStep", "TagA || TagC || TagD");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            Random random = new Random();
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                // ？这一步的用意是什么？
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // 可以看到同一个订单由同一个consumer消费，且同一个订单只存在同一个queue里

//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_1queueId=1, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111065, desc='创建'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_2queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111039, desc='创建'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_3queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103117235, desc='创建'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_2queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111039, desc='付款'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_1queueId=1, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111065, desc='付款'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_2queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111039, desc='推送'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_2queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111039, desc='完成'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_1queueId=1, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103111065, desc='完成'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_3queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103117235, desc='付款'}
//                    consumeThread=ConsumeMessageThread_MyOrderMessageConsumer_3queueId=3, content:2022-12-14 01:34:17Hello RocketMQ OrderStep{orderId=15103117235, desc='完成'}

                    System.out.printf("consumeThread=%s, queueId=%d, content:%s %n", Thread.currentThread().getName(), msg.getQueueId(), new String(msg.getBody()));
//                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                }
                try {
                    TimeUnit.SECONDS.sleep(random.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
