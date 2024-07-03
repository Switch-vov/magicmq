package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.MagicMessage;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * mq for topic.
 *
 * @author switch
 * @since 2024/7/1
 */
@AllArgsConstructor
public class MagicMQ {

    private String topic;
    private LinkedBlockingQueue<MagicMessage> queue = new LinkedBlockingQueue<>();
    private List<MagicListener<?>> listeners = new ArrayList<>();

    public MagicMQ(String topic) {
        this.topic = topic;
    }

    public boolean send(MagicMessage message) {
        boolean offered = queue.offer(message);
        listeners.forEach(listeners -> listeners.onMessage(message));
        return offered;
    }

    // 拉模式获取消息
    @SneakyThrows
    public <T> MagicMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(MagicListener<T> listener) {
        listeners.add(listener);
    }
}
