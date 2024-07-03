package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.Message;
import lombok.AllArgsConstructor;

/**
 * message queue producer.
 *
 * @author switch
 * @since 2024/7/1
 */
@AllArgsConstructor
public class MagicProducer {
    private MagicBroker broker;

    public boolean send(String topic, Message<?> message) {
        return broker.send(topic, message);
    }
}
