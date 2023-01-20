package org.apache.rocketmq.store;


import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class RandomAccessFileTest {

    private final String storeMessage = "Once, there was a chance for me!";

    @Test
    public void TestWrite() throws IOException {
        FileChannel fileChannel = new RandomAccessFile("target/unit_test_store/MappedFileTest/000", "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 64);


        ByteBuffer buf = mappedByteBuffer.slice();
        buf.position(0);
        buf.put(storeMessage.getBytes());

        System.out.println("写入成功");

    }

}
