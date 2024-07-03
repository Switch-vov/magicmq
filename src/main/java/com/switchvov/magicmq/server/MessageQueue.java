package com.switchvov.magicmq.server;

import com.switchvov.magicmq.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * queues.
 *
 * @author switch
 * @since 2024/07/03
 */
@Slf4j
public class MessageQueue {
    private static final Map<String, MessageQueue> QUEUES = new HashMap<>();
    private static final String TEST_TOPIC = "com.switchvov.test";

    static {
        QUEUES.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    private Message<?>[] queue = new Message[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    private Map<String, MessageSubscription> getSubscriptions() {
        return subscriptions;
    }

    public int send(Message<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        if (Objects.isNull(message.getHeaders())) {
            message.setHeaders(new HashMap<>());
        }
        message.getHeaders().put("X-offset", String.valueOf(index));
        queue[index++] = message;
        return index;
    }

    public Message<?> recv(int ind) {
        if (ind <= index) {
            return queue[ind];
        }
        return null;
    }

    public static List<Message<?>> batch(String topic, String consumerId, int size) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        int ind = messageQueue.getSubscriptions().get(consumerId).getOffset();
        int offset = ind + 1;
        List<Message<?>> result = new ArrayList<>();
        Message<?> recv = messageQueue.recv(offset);
        while (Objects.nonNull(recv)) {
            result.add(recv);
            if (result.size() > size) {
                break;
            }
            recv = messageQueue.recv(++offset);
        }
        log.info(" ===>[MagicMQ] batch topic/cid/size = {}/{}/{}", topic, consumerId, size);
        log.info(" ===>[MagicMQ] last message: {}", recv);
        return result;
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static int send(String topic, Message<String> message) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        log.info(" ===>[MagicMQ] send: topic/message = {}/{}", topic, message);
        return messageQueue.send(message);
    }

    public static Message<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        return messageQueue.recv(ind);
    }

    public static Message<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        int ind = messageQueue.getSubscriptions().get(consumerId).getOffset() + 1;
        Message<?> recv = messageQueue.recv(ind);
        log.info(" ===>[MagicMQ] recv: topic/cid/offset = {}/{}/{}", topic, consumerId, ind);
        log.info(" ===>[MagicMQ] recv: message: {}", recv);
        return recv;
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + subscription.getTopic() + " not found");
        }
        log.info(" ===>[MagicMQ] sub = {}", subscription);
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + subscription.getTopic() + " not found");
        }
        log.info(" ===>[MagicMQ] unsub = {}", subscription);
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
            log.info(" ===>[MagicMQ] ack: topic/cid/offset = {}/{}/{}", topic, consumerId, offset);
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }
}
