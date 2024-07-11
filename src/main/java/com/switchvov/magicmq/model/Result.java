package com.switchvov.magicmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Result of MQServer.
 *
 * @author switch
 * @since 2024/07/03
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Result<T> {
    /**
     * 1:success, 0:fail
     */
    private int code;
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<Message<?>> msg(String msg) {
        return new Result<>(1, Message.create(msg, null));
    }

    public static Result<Message<?>> msg(Message<?> msg) {
        return new Result<>(1, msg);
    }

    public static Result<List<Message<?>>> msg(List<Message<?>> msg) {
        return new Result<>(1, msg);
    }

    public static Result<Stat> stat(Stat stat) {
        return new Result<>(1, stat);
    }
}
