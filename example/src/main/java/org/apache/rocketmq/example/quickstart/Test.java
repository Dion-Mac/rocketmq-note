package org.apache.rocketmq.example.quickstart;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) {
        byte[] msg = "HelloLeon".getBytes(StandardCharsets.UTF_8);
        try {
            System.out.println(new String(msg, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
