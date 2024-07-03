package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.Message;

/**
 * message listener.
 *
 * @author switch
 * @since 2024/7/1
 */
public interface MagicListener<T> {
    void onMessage(Message<T> message);
}
