package com.switchvov.magicmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message Subscription.
 *
 * @author switch
 * @since 2024/07/03
 */
@Data
@AllArgsConstructor
public class MessageSubscription {
    private String topic;
    private String consumerId;
    private int offset = -1;
}
