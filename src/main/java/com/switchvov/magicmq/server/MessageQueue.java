package com.switchvov.magicmq.server;

import com.switchvov.magicmq.model.MagicMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * queues.
 *
 * @author switch
 * @since 2024/07/03
 */
public class MessageQueue {
    private static final Map<String, MessageQueue> QUEUES = new HashMap<>();
    private static final String TEST_TOPIC = "com.switchvov.test";

    static {
        QUEUES.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    private MagicMessage<?>[] queue = new MagicMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    private Map<String, MessageSubscription> getSubscriptions() {
        return subscriptions;
    }

    public int send(MagicMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public MagicMessage<?> recv(int ind) {
        if (ind <= index) {
            return queue[ind];
        }
        return null;
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static int send(String topic, String consumerId, MagicMessage<String> message) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        return messageQueue.send(message);
    }

    public static MagicMessage<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (messageQueue.getSubscriptions().containsKey(consumerId)) {
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static MagicMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        int ind = messageQueue.getSubscriptions().get(consumerId).getOffset();
        return messageQueue.recv(ind);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + subscription.getTopic() + " not found");
        }
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + subscription.getTopic() + " not found");
        }
        messageQueue.unsubscribe(subscription);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        MessageSubscription subscription = messageQueue.getSubscriptions().get(consumerId);
        if (offset > subscription.getOffset() && offset <= messageQueue.index) {
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }
}
