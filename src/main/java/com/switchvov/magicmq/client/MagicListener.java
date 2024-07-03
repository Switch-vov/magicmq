package com.switchvov.magicmq.client;

import com.switchvov.magicmq.model.MagicMessage;

/**
 * message listener.
 *
 * @author switch
 * @since 2024/7/1
 */
public interface MagicListener<T> {
    void onMessage(MagicMessage<T> message);
}
