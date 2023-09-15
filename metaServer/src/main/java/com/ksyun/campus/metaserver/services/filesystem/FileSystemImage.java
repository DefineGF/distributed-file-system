package com.ksyun.campus.metaserver.services.filesystem;


import com.ksyun.campus.metaserver.domain.FileNode;
import com.ksyun.campus.metaserver.domain.FileType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * 用以维护整个元数据信息
 */
@Component
@Slf4j
public class FileSystemImage {

    public final static String ROOT_ITEM_PATH = "/";
    public final FileNode ROOT_ITEM = new FileNode();
    private final AtomicInteger fileCount = new AtomicInteger(0); // 文件数量 (不包含目录个数)

    public FileSystemImage() {
        ROOT_ITEM.path = ROOT_ITEM_PATH;
        ROOT_ITEM.mtime = System.currentTimeMillis();
        ROOT_ITEM.type = FileType.Directory;
    }

    /**
     * 创建文件夹:
     * @param path: 格式为 /dir1/dir2
     * @return 创建或已经存在的文件夹
     */
    public FileNode mkdir(String path) {
        path = rectifyPath(path);
        FileNode parent = ROOT_ITEM;
        int index = 0;
        FileNode child = null;
        while (index < path.length()) {
            int nextIndex = path.indexOf("/", index + 1);
            index = nextIndex == -1 ? path.length() : nextIndex;

            String temp = path.substring(0, index++);  // 路径全名
            child = parent.getChild(temp);
            if (child != null) {
                parent = child;
                continue;
            }
            // 创建文件夹
            child = FileNode.createDir(temp);
            parent.addChild(child);
            child.setParent(parent);
            parent = child;
        }
        logFileSystemInfo();
        return child;
    }

    /**
     * 专门用来获取目录，不会获取文件
     * @return 没有则返回 null
     */
    public FileNode getDir(String path) {
        path = rectifyPath(path);

        FileNode parent = ROOT_ITEM;
        int index = 0;
        FileNode child = null;
        while (index < path.length()) {
            int nextIndex = path.indexOf("/", index + 1);
            index = nextIndex == -1 ? path.length() : nextIndex;

            String temp = path.substring(0, index++);
            child = parent.getChild(temp);
            if (child == null) {
                return null;
            }
            parent = child;
        }
        return child == null ? null : (child.type == FileType.File ? null : child);
    }

    /**
     * 递归删除目录
     */
    public void deleteDir(String path) {
        path = rectifyPath(path);
        FileNode dir = getDir(path);
        if (dir.hasChild()) {
            List<FileNode> children = dir.getChildren();
            // 删除子文件
            List<FileNode> fileChildren = children.stream()
                    .filter(file -> file.getType() == FileType.File)
                    .collect(Collectors.toList());
            children.removeAll(fileChildren);

            fileCount.getAndAdd(-1 * fileChildren.size()); // 更新文件个数
            for (FileNode info : children) {
                if (info.getType() == FileType.Directory) {
                    deleteDir(info.getPath());
                }
            }
        }
        FileNode parent = dir.getParent();
        parent.removeChild(dir);

        logFileSystemInfo();
    }

    /**
     * 只创建元数据
     */
    public FileNode createFile(String path) {
        path = rectifyPath(path);

        FileNode file = getFile(path);
        if (file != null) {
            return file; // 已经存在
        }
        fileCount.addAndGet(1);
        int index = path.lastIndexOf("/");
        if (index == -1) {
            // 根目录下创建
            file = FileNode.createFile(path);
            file.setParent(ROOT_ITEM);
            ROOT_ITEM.addChild(file);
        } else {
            FileNode dir = mkdir(path.substring(0, index));
            file = FileNode.createFile(path);
            dir.addChild(file);
            file.setParent(dir);
        }

        logFileSystemInfo();
        return file;
    }

    /**
     * 专门用来获取文件：不会获取目录
     */
    public FileNode getFile(String path) {
        path = rectifyPath(path);
        int index = path.lastIndexOf("/");
        if (index == -1) {
            // 根目录下
            FileNode temp = ROOT_ITEM.getChild(path);
            return temp == null ? null : (temp.type != FileType.File ? null : temp);
        }
        String dirName = path.substring(0, index);
        FileNode dir = getDir(dirName);
        if (dir == null) {
            return null;
        }
        FileNode temp = dir.getChild(path);
        return temp == null ? null : (temp.type == FileType.File ? temp : null);
    }

    public void deleteFile(String path) {
        path = rectifyPath(path);
        FileNode file = getFile(path);
        if (file == null) return;
        fileCount.getAndDecrement();
        file.getParent().removeChild(file); // 删除元数据
        logFileSystemInfo();
    }



    /**
     * 打印文件系统的目录和文件信息
     */
    public void logFileSystemInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("获取文件总数: ").append(fileCount).append("\n");
        logFileSystemInfo(sb, ROOT_ITEM, 0);
        log.info("{}", sb.toString());
    }

    private void logFileSystemInfo(StringBuilder sb, FileNode fileNode, int layer) {
        sb.append("|");

        int count = layer;
        while (count-- > 0) {
            sb.append("----");
        }
        sb.append(fileNode.info()).append("\n");
        if (fileNode.getChildren() == null) {
            return;
        }
        for (FileNode child : fileNode.getChildren()) {
            logFileSystemInfo(sb, child, layer + 1);
        }
        sb.append("\n");
    }

    /**
     * 根据元数据路径生成 唯一文件名
     */
    public static String getFormatPath(String path) {
        path = rectifyPath(path);
        String fileName = path;
        int index = path.lastIndexOf("/");
        if (index != -1) {
            fileName = path.substring(index + 1);
        }
        // 根据唯一path, 通过 hashcode 生成唯一文件名
        return path.hashCode() + "-" + fileName;
    }


    /**
     * 路径格式矫正：win -> unix; 删除前后 /
     */
    public static String rectifyPath(String path) {
        path = path.trim().replace("\\", "/");
        int i = 0;
        for (; i < path.length(); ++i) {
            if (path.charAt(i) != '/') break;
        }
        path = path.substring(i); // 删除最前面的 '/'

        for (i = path.length() - 1; i >= 0; --i) {
            if (path.charAt(i) != '/') break;
        }
        path = path.substring(0, i + 1); // 删除最后面的 '/'
        return path;
    }
}
