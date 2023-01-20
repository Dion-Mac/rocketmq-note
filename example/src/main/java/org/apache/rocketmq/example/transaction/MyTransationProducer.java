package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyTransationProducer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("MyTransationProducer");
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);
        producer.setNamesrvAddr("182.61.6.159:9876");
        producer.start();
        String[] tags = {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            Message message = new Message("TransactionTopic", tags[i % tags.length], "KEY" + i, ("Hello transaction " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.printf("%s%n", sendResult);
            Thread.sleep(10);
        }
        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
