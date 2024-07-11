package com.switchvov.magicmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Message Subscription.
 *
 * @author switch
 * @since 2024/07/03
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Subscription {
    private String topic;
    private String consumerId;
    private int offset = -1;
}
