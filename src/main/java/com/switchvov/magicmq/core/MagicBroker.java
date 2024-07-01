package com.switchvov.magicmq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics.
 *
 * @author switch
 * @since 2024/7/1
 */
public class MagicBroker {
    Map<String, MagicMQ> mqMapping = new ConcurrentHashMap<>(64);

    public MagicMQ find(String topic) {
        return mqMapping.get(topic);
    }

    public MagicMQ createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new MagicMQ(topic));
    }

    public MagicProducer createProducer() {
        return new MagicProducer(this);
    }

    public MagicConsumer<?> createConsumer(String topic) {
        MagicConsumer<?> magicConsumer = new MagicConsumer<>(this);
        magicConsumer.subscribe(topic);
        return magicConsumer;
    }
}
