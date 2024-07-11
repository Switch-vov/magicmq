package com.switchvov.magicmq.server;

import com.switchvov.magicmq.model.Message;
import com.switchvov.magicmq.model.Stat;
import com.switchvov.magicmq.model.Subscription;
import com.switchvov.magicmq.store.Indexer;
import com.switchvov.magicmq.store.Store;
import lombok.Getter;
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

    @Getter
    private Map<String, Subscription> subscriptions = new HashMap<>();
    private String topic;
    @Getter
    private Store store = null;


    public MessageQueue(String topic) {
        this.topic = topic;
        store = new Store(topic);
        store.init();
    }


    public int send(Message<String> message) {
        int offset = store.pos();
        if (Objects.isNull(message.getHeaders())) {
            message.setHeaders(new HashMap<>());
        }
        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);
        return offset;
    }

    public Message<?> recv(int offset) {
        return store.read(offset);
    }

    public static List<Message<?>> batch(String topic, String consumerId, int size) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        int offset = messageQueue.getSubscriptions().get(consumerId).getOffset();
        int nextOffset = 0;
        if (offset > -1) {
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            nextOffset = offset + entry.getLength();
        }
        List<Message<?>> result = new ArrayList<>();
        Message<?> recv = messageQueue.recv(nextOffset);
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

    public static Stat stat(String topic, String consumerId) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        Subscription subscription = messageQueue.getSubscriptions().get(consumerId);
        return new Stat(subscription, messageQueue.getStore().total(), messageQueue.getStore().pos());
    }

    public void subscribe(Subscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(Subscription subscription) {
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

    public static Message<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        return messageQueue.recv(offset);
    }

    public static Message<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = QUEUES.get(topic);
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + topic + " not found");
        }
        if (!messageQueue.getSubscriptions().containsKey(consumerId)) {
            throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        int offset = messageQueue.getSubscriptions().get(consumerId).getOffset();
        int nextOffset = 0;
        if (offset > -1) {
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            nextOffset = offset + entry.getLength();
        }
        Message<?> recv = messageQueue.recv(nextOffset);
        log.info(" ===>[MagicMQ] recv: topic/cid/offset = {}/{}/{}", topic, consumerId, offset);
        log.info(" ===>[MagicMQ] recv: message: {}", recv);
        return recv;
    }

    public static void sub(Subscription subscription) {
        MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
        if (Objects.isNull(messageQueue)) {
            throw new RuntimeException("topic: " + subscription.getTopic() + " not found");
        }
        log.info(" ===>[MagicMQ] sub = {}", subscription);
        messageQueue.subscribe(subscription);
    }

    public static void unsub(Subscription subscription) {
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
        Subscription subscription = messageQueue.getSubscriptions().get(consumerId);
        if (offset > subscription.getOffset() && offset <= Store.LEN) {
            log.info(" ===>[MagicMQ] ack: topic/cid/offset = {}/{}/{}", topic, consumerId, offset);
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }
}
