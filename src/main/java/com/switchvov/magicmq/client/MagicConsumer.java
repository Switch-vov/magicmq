package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.Message;
import lombok.Getter;

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
    @Getter
    private MagicListener listener;

    public MagicConsumer(MagicBroker broker) {
        this.broker = broker;
        this.id = "CID" + ID_GEN.getAndIncrement();
    }

    public String getId() {
        return id;
    }

    public void sub(String topic) {
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
        broker.unsub(topic, id);
    }

    public Message<T> recv(String topic) {
        return broker.recv(topic, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, Message<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic, MagicListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }
}
