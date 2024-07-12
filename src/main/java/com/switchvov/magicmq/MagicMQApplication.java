package com.switchvov.magicmq;

import com.switchvov.magicmq.server.MessageQueue;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MagicMQApplication {

    public static void main(String[] args) {
        SpringApplication.run(MagicMQApplication.class, args);
        MessageQueue.init();
    }

}
