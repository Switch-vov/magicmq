package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.MagicMessage;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer.
 *
 * @author switch
 * @since 2024/7/1
 */
public class MagicConsumer<T> {
    private static final AtomicInteger ID_GEN = new AtomicInteger();

    private String id;
    private MagicBroker broker;
    private String topic;
    private MagicMQ mq;

    public MagicConsumer(MagicBroker broker) {
        this.broker = broker;
        this.id = "CID" + ID_GEN.getAndIncrement();
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
