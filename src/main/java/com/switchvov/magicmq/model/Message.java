package com.switchvov.magicmq.model;

import lombok.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * magic message model.
 *
 * @author switch
 * @since 2024/7/1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message<T> {
    private static final AtomicLong ID_GEN = new AtomicLong();

    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>();

    public static long getId() {
        return ID_GEN.getAndIncrement();
    }

    public static Message<?> create(String body, Map<String, String> headers) {
        return new Message<>(getId(), body, headers);
    }
}
