package com.switchvov.magicmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * stats for mq.
 *
 * @author switch
 * @since 2024/07/11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    private Subscription subscription;
    private int total;
    private int position;
}
