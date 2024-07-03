package com.switchvov.magicmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Result of MQServer.
 *
 * @author switch
 * @since 2024/07/03
 */
@Data
@AllArgsConstructor
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

    public static Result<MagicMessage<?>> msg(String msg) {
        return new Result<>(1, MagicMessage.create(msg, null));
    }

    public static Result<MagicMessage<?>> msg(MagicMessage<?> msg) {
        return new Result<>(1, msg);
    }
}
