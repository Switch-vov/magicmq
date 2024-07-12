package com.switchvov.magicmq.store;

import com.switchvov.magicmq.constant.Constants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * file mapped for topic store
 *
 * @author switch
 * @since 2024/07/11
 */
@Slf4j
public class FileMappedImpl implements FileMapped {
    private final String topic;
    private final String filePath;
    private final String fileId;
    private int startOffset;
    @Getter
    private int endOffset;

    private FileChannel channel = null;
    private MappedByteBuffer mappedByteBuffer = null;

    public FileMappedImpl(String topic, String filePath, String fileId, int startOffset) {
        this.topic = topic;
        this.filePath = filePath;
        this.fileId = fileId;
        this.startOffset = startOffset;
    }


    @Override
    public void init() throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }

        Path path = Paths.get(file.getAbsolutePath());
        channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, Constants.INIT_POSITION, Constants.LEN);

        // 读取索引
        ByteBuffer buffer = mappedByteBuffer.asReadOnlyBuffer();

        // 判断是否有数据
        // 读前10位，转成int=len，看是不是大于0，往后翻len的长度，就是下一条记录，
        // 重复上一步，一直到0为止，找到数据结尾
        byte[] header = new byte[PackageCodec.HEADER_LEN];
        buffer.get(header);
        int pos = 0;
        int msgLen = PackageCodec.decodeHeader(header);
        while (msgLen > 0) {
            int len = msgLen + PackageCodec.HEADER_LEN;
            int offset = pos + startOffset;
            Indexer.addEntry(topic, offset, len, fileId);
            pos += len;
            if (pos >= Constants.LEN) {
                break;
            }
            log.debug(" ===>[MagicMQ] next = {}", offset);
            buffer.position(pos);
            buffer.get(header);
            msgLen = PackageCodec.decodeHeader(header);
        }

        buffer = null;
        mappedByteBuffer.position(pos);
        endOffset = startOffset + pos;
        log.info(" ===>[MagicMQ] init pos = {}, offset = {}", pos, endOffset);
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(channel)) {
            try {
                channel.close();
            } catch (IOException e) {
                log.error("close channel error, topic: {},fileId: {}, filePath:{}", topic, fileId, filePath, e);
            }
        }
        mappedByteBuffer = null;
    }

    @Override
    public int write(byte[] msg) {
        int position = mappedByteBuffer.position();
        mappedByteBuffer.put(ByteBuffer.wrap(msg));
        int beforeOffset = startOffset + position;
        Indexer.addEntry(topic, beforeOffset, msg.length, fileId);
        endOffset = beforeOffset + msg.length;
        return beforeOffset;
    }


    @Override
    public byte[] read(Indexer.Entry entry) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        int position = entry.getOffset() - startOffset;
        if (position < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(position));
        }
        readOnlyBuffer.position(position);
        byte[] msgPackage = new byte[entry.getLength()];
        readOnlyBuffer.get(msgPackage, 0, entry.getLength());
        return msgPackage;
    }

    @Override
    public int getStorage() {
        return mappedByteBuffer.position();
    }

    @Override
    public void resetStartOffset(int offset) {
        startOffset = offset;
    }

}
