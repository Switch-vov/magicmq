package com.switchvov.magicmq.util;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
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

    public static void main(String[] args) throws IOException {
        List<String> paths = findPaths("./", "magic-test.*.dat");
        for (String path : paths) {
            File file = new File(path);
            System.out.println("file:" + file.getAbsolutePath());
        }
        File file = new File("./magic-test.1.dat");
        System.out.println("file-2:" + file.getAbsolutePath());
        paths.stream().forEach(System.out::println);
    }
}
