package com.switchvov.magicmq.demo;

import com.switchvov.magicmq.client.MagicBroker;
import com.switchvov.magicmq.client.MagicConsumer;
import com.switchvov.magicmq.model.MagicMessage;
import com.switchvov.magicmq.client.MagicProducer;
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

        String topic = "magic.order";
        MagicBroker broker = new MagicBroker();
        broker.createTopic(topic);

        MagicProducer producer = broker.createProducer();
        MagicConsumer<?> consumer = broker.createConsumer(topic);
        consumer.subscribe(topic);
        consumer.listen(message -> log.info(" ===>[MagicMQ] onMessage: {}", message));

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new MagicMessage<>(ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            MagicMessage<?> message = consumer.poll(1000);
            log.info(" ===>[MagicMQ] poll message: {}", message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new MagicMessage<>(ids++, order, null));
                log.info(" ===>[MagicMQ] send message ok: {}", order);
            }
            if (c == 'c') {
                MagicMessage<?> message = consumer.poll(1000);
                log.info(" ===>[MagicMQ] poll message ok: {}", message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new MagicMessage<>(ids++, order, null));
                }
                log.info(" ===>[MagicMQ] send 10 orders...");
            }
        }
    }
}
