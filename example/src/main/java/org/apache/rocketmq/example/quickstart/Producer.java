/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("producerGroup");
        DefaultMQProducer producerb = new DefaultMQProducer("producerGroup");
        producer.setNamesrvAddr("182.61.6.159:9876");
        producer.setSendMsgTimeout(8000);
        producerb.setNamesrvAddr("182.61.6.159:9876");
        producerb.setSendMsgTimeout(8000);
//        producer.setInstanceName("");
//        producer.setUnitName("");

        producer.setRetryTimesWhenSendFailed(1);
        producerb.setRetryTimesWhenSendFailed(1);
//        producer.setRetryTimesWhenSendAsyncFailed(1);
//        producer.setClientCallbackExecutorThreads(2);

        /*b
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Launch the instance.
         */
        producer.start();
        producerb.start();


        for (int i = 0; i < 1000000; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message("TopicTestD" /* Topic */,
                        "TagA" /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);
//                SendResult sendResultb = producerb.send(msg);
                System.out.printf("%s%n", sendResult);

//                System.out.println("消息状态：" + sendResult.getSendStatus());
//                System.out.println("消息id：" + sendResult.getMsgId());
//                System.out.println("消息queue：" + sendResult.getMessageQueue());
//                System.out.println("消息offset：" + sendResult.getQueueOffset());
//
//                System.out.println("消息b状态：" + sendResultb.getSendStatus());
//                System.out.println("消息bid：" + sendResultb.getMsgId());
//                System.out.println("消息bqueue：" + sendResultb.getMessageQueue());
//                System.out.println("消息boffset：" + sendResultb.getQueueOffset());

//                producer.send(msg, new SendCallback() {
//                    @Override
//                    public void onSuccess(SendResult sendResult) {
//                        System.out.println("异步消息状态：" + sendResult.getSendStatus());
//                        System.out.println("异步消息id：" + sendResult.getMsgId());
//                        System.out.println("异步消息queue：" + sendResult.getMessageQueue());
//                        System.out.println("异步消息offset：" + sendResult.getQueueOffset());
//                    }
//                    @Override
//                    public void onException(Throwable e) {
//                        System.out.println("发送失败！" + e);
//                    }
//                });
                /*
                 * There are different ways to send message, if you don't care about the send result,you can use this way
                 * {@code
                 * producer.sendOneway(msg);
                 * }
                 */

                /*
                 * if you want to get the send result in a synchronize way, you can use this send method
                 * {@code
                 * SendResult sendResult = producer.send(msg);
                 * System.out.printf("%s%n", sendResult);
                 * }
                 */

                /*
                 * if you want to get the send result in a asynchronize way, you can use this send method
                 * {@code
                 *
                 *  producer.send(msg, new SendCallback() {
                 *  @Override
                 *  public void onSuccess(SendResult sendResult) {
                 *      // do something
                 *  }
                 *
                 *  @Override
                 *  public void onException(Throwable e) {
                 *      // do something
                 *  }
                 *});
                 *
                 *}
                 */
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }




        /*
         * Shut down once the producer instance is not longer in use.
         */
//        producer.shutdown();
    }

}
