package com.ksyun.campus.metaserver.services.filesystem;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.FileNode;
import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.zookeeper.ZKConnector;
import com.ksyun.campus.zookeeper.ZookeeperUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 *  用于关联在Zookeeper中的元数据:
 *  在 zk-fs 下创建 node
 */
@Slf4j
@Component
public class ZKFileSystem extends ZKConnector {
    public final static String ROOT = "/zk-fs";

    public String flag = "";

    @Resource
    private FileSystemImage fileSystemImage;


    public ZKFileSystem(@Value("${zookeeper.addr}") String zkInfo) {
        super(zkInfo);
        init();
    }

    private void init() {
        try {
            // 创建根节点
            ZookeeperUtil.createNodeDirectly(this.client, ROOT, new byte[0], CreateMode.PERSISTENT);
        } catch (Exception e) {
            log.error("创建 zk 文件根节点失败: {}", e.getMessage(), e);
        }
        addListener();
    }

    private void addListener() {
        TreeCache treeCache = new TreeCache(client, ROOT);
        try {
            treeCache.start();
            treeCache.getListenable().addListener((client1, event) -> {
                // 处理核心监听事件
                handleFSEvent(event);
            });
        } catch (Exception e) {
            log.error("zk 根节点添加监听失败: {}", e.getMessage(), e);
        }
    }

    public void createDirectory(String nodePath, StatInfo statInfo) throws Exception {
        try {
            String json = new ObjectMapper().writeValueAsString(statInfo);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            ZookeeperUtil.createNodeRecursively(client, ROOT + nodePath, value, CreateMode.PERSISTENT);
        } catch (JsonProcessingException e) {
            log.error("json 解析失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 将文件元数据信息，写入zookeeper节点内容
     */
    public void createFile(String nodePath, StatInfo statInfo) throws Exception {
        try {
            String json = new ObjectMapper().writeValueAsString(statInfo);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            ZookeeperUtil.createNodeRecursively(client, ROOT + nodePath, value, CreateMode.PERSISTENT);
        } catch (JsonProcessingException e) {
            log.error("json 解析失败: {}", e.getMessage(), e);
        }
    }

    public void delete(String nodePath) throws Exception {
        if (client.checkExists().forPath(nodePath) != null) {
            ZookeeperUtil.deleteNode(client, ROOT + nodePath);
        }
    }

    public void update(String path, StatInfo statInfo) throws Exception {
        String newPath = ROOT + path;
        if (client.checkExists().forPath(newPath) == null) {
            log.warn("zk 更新 {} 文件失败：节点不存在", newPath);
            return;
        }
        String newJson = new ObjectMapper().writeValueAsString(statInfo);
        ZookeeperUtil.updateNode(client, newPath, newJson);
    }

    private void handleFSEvent(TreeCacheEvent event) {
        TreeCacheEvent.Type type = event.getType();
        if (event.getData() != null) {
            log.info("zk {} 触发监听, 类型 = {}, ", event.getData().getPath(), type);
            try {
                if (Arrays.equals(event.getData().getData(), new byte[0])) {
                    return;
                }
                String data = new String(event.getData().getData(), StandardCharsets.UTF_8);
                StatInfo statInfo = new ObjectMapper().readValue(data, StatInfo.class);
                if (type == TreeCacheEvent.Type.NODE_ADDED) {
                    // 增加
                    if (statInfo.getType() == FileType.Directory) {
                        FileNode fileNode = fileSystemImage.mkdir(statInfo.getPath());
                        fileNode.setMtime(statInfo.getMtime());
                        fileNode.setSize(statInfo.getSize());
                    } else if (statInfo.getType() == FileType.File) {
                        FileNode fileNode = fileSystemImage.createFile(statInfo.getPath());
                        fileNode.setMtime(statInfo.getMtime());
                        fileNode.setSize(statInfo.getSize());
                        fileNode.setReplicaDataList(statInfo.getReplicaData());
                    }
                } else if (type == TreeCacheEvent.Type.NODE_REMOVED) {
                    // 删除
                    if (statInfo.getType() == FileType.Directory) {
                        fileSystemImage.deleteDir(statInfo.getPath());
                    } else if (statInfo.getType() == FileType.File) {
                        fileSystemImage.deleteFile(statInfo.getPath());
                    }
                } else if (type == TreeCacheEvent.Type.NODE_UPDATED) {
                    // 修改
                    FileNode fileNode;
                    FileType fileType = statInfo.getType();
                    if (fileType == FileType.Directory || fileType == FileType.File) {
                        fileNode = fileType == FileType.Directory ? fileSystemImage.getDir(statInfo.getPath()) : fileSystemImage.getFile(statInfo.getPath());
                        fileNode.setSize(statInfo.getSize());
                        fileNode.setMtime(statInfo.getMtime());
                    }
                }

            } catch (JsonProcessingException e) {
                log.error("zk 解析json失败: {}", e.getMessage(), e);
            }
        } else {
            log.warn("zk 监听，数据为空!");
        }
    }
}
