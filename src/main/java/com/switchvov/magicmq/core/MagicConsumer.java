package com.switchvov.magicmq.core;

import java.util.Objects;

/**
 * message consumer.
 *
 * @author switch
 * @since 2024/7/1
 */
public class MagicConsumer<T> {
    private MagicBroker broker;
    private String topic;
    private MagicMQ mq;

    public MagicConsumer(MagicBroker broker) {
        this.broker = broker;
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(this.topic);
        if (Objects.isNull(mq)) {
            throw new RuntimeException("topic not found");
        }
    }

    public MagicMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public void listen(MagicListener<T> listener) {
        mq.addListener(listener);
    }
}
