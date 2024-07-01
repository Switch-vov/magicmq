package com.switchvov.magicmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * magic message model.
 *
 * @author switch
 * @since 2024/7/1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MagicMessage<T> {
    private Long id;
    private T body;
    private Map<String, String> headers;
}
