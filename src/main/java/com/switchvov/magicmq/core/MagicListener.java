package com.switchvov.magicmq.core;

/**
 * message listener.
 *
 * @author switch
 * @since 2024/7/1
 */
public interface MagicListener<T> {
    void onMessage(MagicMessage<T> message);
}
