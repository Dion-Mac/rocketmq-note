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
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerMultithread {
    public static void main(String[] args) throws InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("producerGroup");
        producer.setNamesrvAddr("192.168.1.2:9876");
        producer.setSendMsgTimeout(8000);

        int threadCount = 32;
        CountDownLatch count = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        int messageCount = 100000;

        for (int i = 0; i < threadCount; i++) {
            ProduceMessageTask task = new ProduceMessageTask(producer, messageCount, count, errorCount);
            new Thread(task).start();
        }

        count.await();

        producer.shutdown();
    }

    static class ProduceMessageTask implements Runnable {

        private DefaultMQProducer producer;

        private int count;

        private CountDownLatch countDownLatch;

        private AtomicInteger errorCount;

        public ProduceMessageTask(DefaultMQProducer producer, int count, CountDownLatch countDownLatch, AtomicInteger errorCount) {
            this.producer = producer;
            this.count = count;
            this.countDownLatch = countDownLatch;
            this.errorCount = errorCount;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + "start");
            for (int i = 0; i < count; i++) {
                try {
                    Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    if(count % 100 == 0){
                        System.out.println(sendResult);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                }
            }

            countDownLatch.countDown();
            System.out.println(Thread.currentThread().getName() + "end");
        }
    }

}
