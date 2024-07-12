package com.switchvov.magicmq.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.switchvov.magicmq.model.Message;
import com.switchvov.magicutils.JsonUtils;

import java.nio.charset.StandardCharsets;

/**
 * package codec.
 *
 * @author switch
 * @since 2024/07/12
 */
public class PackageCodec {
    public static final int HEADER_LEN = 10;

    public static byte[] encode(Message<String> mm) {
        String msg = JsonUtils.toJson(mm);
        int len = msg.getBytes(StandardCharsets.UTF_8).length;
        // 将消息长度格式化为长度10位的字符串，使用前导零填充，比如长度为10，则格式化字符为 0000000010
        String prefix = String.format("%010d", len);
        return (prefix + msg).getBytes(StandardCharsets.UTF_8);
    }

    public static Message<String> decode(byte[] allBytes) {
        String all = new String(allBytes, StandardCharsets.UTF_8);
        String msg = all.substring(HEADER_LEN);
        return JsonUtils.fromJson(msg, new TypeReference<Message<String>>() {
        });
    }

    public static int decodeHeader(byte[] headerBytes) {
        String header = new String(headerBytes, StandardCharsets.UTF_8);
        if (header.trim().isBlank()) {
            return 0;
        }
        return Integer.parseInt(header);
    }
}
