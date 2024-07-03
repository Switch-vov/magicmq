package com.switchvov.magicmq.demo;

import com.switchvov.magicmq.client.MagicBroker;
import com.switchvov.magicmq.client.MagicConsumer;
import com.switchvov.magicmq.client.MagicProducer;
import com.switchvov.magicmq.model.Message;
import com.switchvov.magicutils.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * mq demo for order.
 *
 * @author switch
 * @since 2024/7/1
 */
@Slf4j
public class MagicMQDemo {
    public static void main(String[] args) throws IOException {
        long ids = 0;

        String topic = "com.switchvov.test";
        MagicBroker broker = MagicBroker.getDEFAULT();

        MagicProducer producer = broker.createProducer();
        MagicConsumer<?> consumer = broker.createConsumer(topic);

        consumer.listen(topic, message -> log.info(" ===>[MagicMQ] onMessage: {}", message));

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new Message<>(ids++, JsonUtils.toJson(order), null));
        }

        for (int i = 0; i < 10; i++) {
            Message<?> message = consumer.recv(topic);
            consumer.ack(topic, message);
            log.info(" ===>[MagicMQ] recv message: {}", message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new Message<>(ids++, JsonUtils.toJson(order), null));
                log.info(" ===>[MagicMQ] send message ok: {}", order);
            }
//            if (c == 'c') {
//                Message<?> message = consumer.recv(topic);
//                if (Objects.nonNull(message)) {
//                    consumer.ack(topic, message);
//                }
//                log.info(" ===>[MagicMQ] recv message ok: {}", message);
//            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new Message<>(ids++, JsonUtils.toJson(order), null));
                }
                log.info(" ===>[MagicMQ] send 10 orders...");
            }
        }
    }
}
