package com.switchvov.magicmq.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.switchvov.magicmq.model.Message;
import com.switchvov.magicmq.model.Result;
import com.switchvov.magicmq.model.Stat;
import com.switchvov.magicutils.HttpUtils;
import com.switchvov.magicutils.JsonUtils;
import com.switchvov.magicutils.ThreadUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Objects;

/**
 * broker for topics.
 *
 * @author switch
 * @since 2024/7/1
 */
@Slf4j
public class MagicBroker {
    public static final String brokerUrl = "http://localhost:8765/magicmq";

    @Getter
    public static final MagicBroker DEFAULT = new MagicBroker();

    private MagicBroker() {

    }

    static {
        init();
    }

    private static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, MagicConsumer<?>> consumerMap = getDEFAULT().getConsumers();
            consumerMap.forEach((topic, consumers) -> consumers.forEach(consumer -> {
                Message<?> recv = consumer.recv(topic);
                if (Objects.isNull(recv)) {
                    return;
                }
                try {
                    consumer.getListener().onMessage(recv);
                    consumer.ack(topic, recv);
                } catch (Exception e) {
                    log.error("onMessage error", e);
                }
            }));
        }, 100, 1000);
    }

    @Getter
    private MultiValueMap<String, MagicConsumer<?>> consumers = new LinkedMultiValueMap<>();


    public MagicProducer createProducer() {
        return new MagicProducer(this);
    }

    public MagicConsumer<?> createConsumer(String topic) {
        MagicConsumer<Object> consumer = new MagicConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, Message<?> message) {
        log.info(" ===>[MagicMQ] send topic/message: {}/{}", topic, message);
        Result<String> result = HttpUtils.httpPost(JsonUtils.toJson(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        log.info(" ===>[MagicMQ] send result: {}", result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        log.info(" ===>[MagicMQ] sub topic/cid: {}/{}", topic, cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + cid, new TypeReference<Result<String>>() {
        });
        log.info(" ===>[MagicMQ] sub result: {}", result);
    }

    public void unsub(String topic, String cid) {
        log.info(" ===>[MagicMQ] unsub topic/cid: {}/{}", topic, cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>() {
                });
        log.info(" ===>[MagicMQ] unsub result: {}", result);
    }

    public <T> Message<T> recv(String topic, String cid) {
        log.info(" ===>[MagicMQ] recv topic/cid: {}/{}", topic, cid);
        Result<Message<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<Message<String>>>() {
                });
        log.info(" ===>[MagicMQ] recv result: {}", result);
        return (Message<T>) result.getData();
    }

    public boolean ack(String topic, String cid, int offset) {
        log.info(" ===>[MagicMQ] ack topic/cid/offset: {}/{}/{}", topic, cid, offset);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset,
                new TypeReference<Result<String>>() {
                });
        log.info(" ===>[MagicMQ] ack result: {}", result);
        return result.getCode() == 1;
    }

    public void addConsumer(String topic, MagicConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }

    // GET http://localhost:8765/magicmq/stat?t=com.switchvov.test&cid=CID0
    public Stat stat(String topic, String cid) {
        log.info(" ===>[MagicMQ] stat topic/cid: {}/{}", topic, cid);
        Result<Stat> result = HttpUtils.httpGet(brokerUrl + "/stat?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<Stat>>() {
                });
        log.info(" ===>[MagicMQ] stat result: {}", result);
        return result.getData();
    }
}
