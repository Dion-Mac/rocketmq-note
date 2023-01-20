package org.apache.rocketmq.example.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MySplitBatchProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("MySplitBatchProducer");
        producer.setNamesrvAddr("182.61.6.159:9876");
        producer.start();

        //large batch
        String topic = "SplitBatchTest";
        List<Message> messages = new ArrayList<>(100 * 1000);
        messages.add(new Message(topic, "Tag", "OrderIDAAAAAAA", new byte[1000 * 1001]));
        for (int i = 0; i < 100 * 1000; i++) {
            messages.add(new Message(topic, "Tag", "OrderID" + i, ("Hello world " + i).getBytes()));
        }
        MyListSplitter mySplitter = new MyListSplitter(messages);
        while (mySplitter.hasNext()) {
            List<Message> listItem = mySplitter.next();
            producer.send(listItem);
        }
        producer.shutdown();
    }
}
class MyListSplitter implements Iterator<List<Message>> {
    private final int SIZE_LIMIT = 1000 * 1000;
    private final List<Message> messages;
    private int currIndex;

    public MyListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            tmpSize += 20; //增加日志的开销20字节
            if (tmpSize > SIZE_LIMIT) {
                //单个消息超过最大的限制，则忽略，否则会阻塞分裂的进程
                if (nextIndex - currIndex == 0) {
                    //假如下一个子列表没有元素,则添加这个子列表然后退出循环,否则只是退出循环
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                totalSize += tmpSize;
            }
        }
        //subList包括currIndex, 不包括nextIndex
        List<Message> subList = this.messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}