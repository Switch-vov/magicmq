package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.MagicMessage;
import lombok.AllArgsConstructor;

import java.util.Objects;

/**
 * message queue producer.
 *
 * @author switch
 * @since 2024/7/1
 */
@AllArgsConstructor
public class MagicProducer {
    private MagicBroker broker;

    public boolean send(String topic, MagicMessage<?> message) {
        MagicMQ mq = broker.find(topic);
        if (Objects.isNull(mq)) {
            throw new RuntimeException("topic not found");
        }
        return mq.send(message);
    }
}
