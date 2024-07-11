package com.switchvov.magicmq.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.switchvov.magicmq.model.Message;
import com.switchvov.magicutils.JsonUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Message Store.
 *
 * @author switch
 * @since 2024/07/11
 */
@Slf4j
public class Store {
    public static final int LEN = 1024 * 10;

    private static final int INIT_POSITION = 0;
    private static final String FILE_SUFFIX = ".dat";

    private final String topic;
    private MappedByteBuffer mappedByteBuffer = null;


    public Store(String topic) {
        this.topic = topic;
    }

    @SneakyThrows
    public void init() {
        File file = new File(topic + FILE_SUFFIX);
        if (!file.exists()) {
            file.createNewFile();
        }

        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, INIT_POSITION, LEN);

        // 读取索引
        ByteBuffer buffer = mappedByteBuffer.asReadOnlyBuffer();

        // 判断是否有数据
        // 读前10位，转成int=len，看是不是大于0，往后翻len的长度，就是下一条记录，
        // 重复上一步，一直到0为止，找到数据结尾
        byte[] header = new byte[StoreEntryCodec.HEADER_LEN];
        buffer.get(header);
        int pos = 0;
        int msgLen = StoreEntryCodec.decodeHeader(header);
        while (msgLen > 0) {
            int len = msgLen + StoreEntryCodec.HEADER_LEN;
            Indexer.addEntry(topic, pos, len);
            pos += len;
            log.debug(" ===>[MagicMQ] next = {}", pos);
            buffer.position(pos);
            buffer.get(header);
            msgLen = StoreEntryCodec.decodeHeader(header);
        }
        buffer = null;
        log.info(" ===>[MagicMQ] init pos = {}", pos);
        mappedByteBuffer.position(pos);


        // TODO:code 2.如果总数据 > 10M，使用多个数据文件的list来管理持久化数据
        // 需要创建第二个数据文件，怎么来管理多个数据文件。
    }

    public int write(Message<String> mm) {
        byte[] msgPackage = StoreEntryCodec.encode(mm);
        log.debug(" ===>[MagicMQ] write pos -> {}, msg package: {}",
                mappedByteBuffer.position(), new String(msgPackage, StandardCharsets.UTF_8));
        int position = mappedByteBuffer.position();
        Indexer.addEntry(topic, position, msgPackage.length);
        mappedByteBuffer.put(ByteBuffer.wrap(msgPackage));
        return position;
    }

    public Message<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        if (entry == null) {
            return null;
        }
        readOnlyBuffer.position(entry.getOffset());

        byte[] msgPackage = new byte[entry.getLength()];
        readOnlyBuffer.get(msgPackage, 0, entry.getLength());
        log.debug(" ===>[MagicMQ] read pos -> {}, msg package: {}",
                entry.getOffset(), new String(msgPackage, StandardCharsets.UTF_8));
        return StoreEntryCodec.decode(msgPackage);
    }

    public int pos() {
        return mappedByteBuffer.position();
    }

    public int total() {
        return Indexer.getEntries(topic).size();
    }

    private static class StoreEntryCodec {
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
}
