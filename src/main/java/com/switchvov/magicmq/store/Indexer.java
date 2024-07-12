package com.switchvov.magicmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;

/**
 * entry indexer.
 *
 * @author switch
 * @since 2024/07/11
 */
@Slf4j
public class Indexer {
    private static final MultiValueMap<String, Entry> INDEXES = new LinkedMultiValueMap<>();
    private static final Map<String, Map<Integer, Entry>> MAPPINGS = new HashMap<>();

    public static void addEntry(String topic, int offset, int len, String fileId) {
        log.debug(" ===>[MagicMQ] add entry topic/offset/length/fileId = {}/{}/{}/{}", topic, offset, len, fileId);
        Entry value = new Entry(offset, len, fileId);
        INDEXES.add(topic, value);
        putMapping(topic, offset, value);
    }

    private static void putMapping(String topic, int offset, Entry value) {
        MAPPINGS.computeIfAbsent(topic, k -> new HashMap<>()).put(offset, value);
    }

    public static List<Entry> getEntries(String topic) {
        return INDEXES.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        return Optional.ofNullable(MAPPINGS.get(topic))
                .map(mapping -> mapping.get(offset))
                .orElse(null);
    }

    @Data
    @AllArgsConstructor
    public static class Entry {
        private int offset;
        private int length;
        private String fileId;
    }
}
