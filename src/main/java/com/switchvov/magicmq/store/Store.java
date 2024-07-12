package com.switchvov.magicmq.store;

import com.switchvov.magicmq.model.Message;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Message Store.
 *
 * @author switch
 * @since 2024/07/11
 */
@Slf4j
public class Store {
    private final String topic;
    private FileMapped fileMapped = null;


    public Store(String topic) {
        this.topic = topic;
    }

    @SneakyThrows
    public void init() {
        fileMapped = new FileMappeds(topic);
        fileMapped.init();
    }

    public int write(Message<String> mm) {
        byte[] msgPackage = PackageCodec.encode(mm);
        int offset = fileMapped.write(msgPackage);
        log.debug(" ===>[MagicMQ] write pos -> {}, msg package: {}",
                offset, new String(msgPackage, StandardCharsets.UTF_8));
        return offset;
    }

    public Message<String> read(int offset) {
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        if (Objects.isNull(entry)) {
            return null;
        }
        byte[] msgPackage = fileMapped.read(entry);
        log.debug(" ===>[MagicMQ] read pos -> {}, msg package: {}",
                entry.getOffset(), new String(msgPackage, StandardCharsets.UTF_8));
        return PackageCodec.decode(msgPackage);
    }

    public int pos() {
        return fileMapped.getEndOffset();
    }


    public int total() {
        return Indexer.getEntries(topic).size();
    }

}
