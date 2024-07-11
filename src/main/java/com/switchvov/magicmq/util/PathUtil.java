package com.switchvov.magicmq.util;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author switch
 * @since 2024/07/11
 */
public class PathUtil {
    /**
     * @param path        指定路径
     * @param filePattern 文件模式
     * @return 绝对文件路径列表
     * @throws IOException
     */
    public static List<String> findPaths(String path, String filePattern) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("file:" + path + filePattern);
        List<String> filePaths = new ArrayList<>();
        for (Resource resource : resources) {
            filePaths.add(resource.getFile().getAbsolutePath());
        }
        return filePaths;
    }
}
