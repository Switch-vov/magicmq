package com.switchvov.magicmq.store;

import com.switchvov.magicmq.constant.Constants;
import com.switchvov.magicmq.util.PathUtil;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * a group file mapped for topic store
 * <p>
 * TODO:code: 优化项: 打开太多文件，会消耗大量内存，应做文件管理策略
 *
 * @author switch
 * @since 2024/07/11
 */
public class FileMappeds implements FileMapped {

    private final Map<String, FileMapped> fileMappedMap;
    private final String topic;
    private FileMapped currentMapped;
    private FileMapped nextMapped;
    @Getter
    private int endOffset;

    private AtomicInteger fileIdGen;
    private Pattern filePathPattern;

    public FileMappeds(String topic) {
        this.fileMappedMap = new LinkedHashMap<>();
        this.endOffset = 0;
        this.topic = topic;
        this.fileIdGen = new AtomicInteger();
        this.filePathPattern = Pattern.compile(".*/" + topic + "\\.(\\d+)" + Constants.FILE_SUFFIX);
    }

    @Override
    public void init() throws IOException {
        String pattern = topic + ".*" + Constants.FILE_SUFFIX;
        List<String> paths = PathUtil.findPaths(Constants.DEFAULT_PATH, pattern);
        if (paths.isEmpty()) {
            paths.add(genFilePath(this.fileIdGen.get()));
        }
        this.fileIdGen.set(paths.size());

        // 遍历文件列表，将 offset 通过 start offset 和 end offset 串起来
        for (String path : paths) {
            String fileId = getFileIdFromPath(path);
            FileMappedImpl mapped = new FileMappedImpl(topic, path, fileId, endOffset);
            fileMappedMap.put(fileId, mapped);
            mapped.init();
            endOffset = mapped.getEndOffset();
            currentMapped = mapped;
        }

    }

    private String genFilePath(int fileId) {
        return Constants.DEFAULT_PATH + topic + "." + String.format("%010d", fileId) + Constants.FILE_SUFFIX;
    }

    private String getFileIdFromPath(String path) {
        Matcher matcher = filePathPattern.matcher(path);
        if (matcher.find()) {
            // 获取匹配的组
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Invalid file path format: " + path);

    }

    @Override
    public void destroy() {
        fileMappedMap.values().forEach(FileMapped::destroy);
    }

    @Override
    public int write(byte[] msg) {
        // 当当前文件使用空间超过一定比例时，预先创建下一个文件
        checkAndPreCreateFile();
        // 当当前文件不足以存下新增
        if (currentMapped.getStorage() + msg.length > Constants.LEN) {
            rotateFile();
        }
        int offset = currentMapped.write(msg);
        endOffset = currentMapped.getEndOffset();
        return offset;
    }

    @Override
    public byte[] read(Indexer.Entry entry) {
        String fileId = entry.getFileId();
        FileMapped mapped = fileMappedMap.get(fileId);
        if (Objects.isNull(mapped)) {
            throw new IllegalArgumentException("Invalid fileId: " + fileId + " entry: " + entry);
        }
        return mapped.read(entry);
    }

    @SneakyThrows
    private void rotateFile() {
        if (Objects.isNull(nextMapped)) {
            nextMapped = createNewFile();
        }
        currentMapped = nextMapped;
        currentMapped.resetStartOffset(endOffset);
        currentMapped.init();

        nextMapped = null;
    }

    /**
     * 预先创建下一个文件
     */
    private void checkAndPreCreateFile() {
        if (Objects.isNull(nextMapped) &&
                currentMapped.getStorage() >= Constants.LEN * Constants.DEFAULT_CREATE_NEXT_FILE_LIMIT) {
            nextMapped = createNewFile();
        }
    }

    private FileMapped createNewFile() {
        String path = genFilePath(fileIdGen.getAndIncrement());
        String fileId = getFileIdFromPath(path);
        FileMappedImpl mapped = new FileMappedImpl(topic, path, fileId, endOffset);
        fileMappedMap.put(fileId, mapped);
        return mapped;
    }

    @Override
    public int getStorage() {
        return fileMappedMap.values().stream().mapToInt(FileMapped::getStorage).sum();
    }
}
