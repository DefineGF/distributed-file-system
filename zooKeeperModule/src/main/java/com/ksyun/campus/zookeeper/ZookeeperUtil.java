package com.ksyun.campus.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;

@Slf4j
public class ZookeeperUtil {
    private final static int SESSION_TIMEOUT = 3 * 1000; // 会话超时时间


    /**
     * 创建 CuratorFramework client：
     *      getCuratorFramework("127.0.0.1:9000,127.0.0.1:9001")
     */
    public static CuratorFramework getCuratorFramework(String zkInfo) {
        return getCuratorFramework(zkInfo, SESSION_TIMEOUT);
    }

    public static CuratorFramework getCuratorFramework(String zkInfo, int sessionTimeout) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3); // 重试策略

        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkInfo)
                .sessionTimeoutMs(sessionTimeout)       // 会话过期：    默认60s
                .connectionTimeoutMs(10000)             // 连接过期时间： 默认15s
                .retryPolicy(retryPolicy)               // 重试策略
                .build();
        client.start(); // 阻塞启动
        return client;
    }

    public static String createNodeDirectly(CuratorFramework client, String path, byte[] value, CreateMode mode) throws Exception {
        if (client == null) return null;
        if (client.checkExists().forPath(path) == null) {
            client.create().withMode(mode).forPath(path, value);
        }
        return path;
    }

    public static String createNodeRecursively(CuratorFramework client, String path, byte[] value, CreateMode mode) throws Exception {
        if (client == null) return null;

        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().withMode(mode).forPath(path, value);
        }
        return path;
    }

    public static void updateNode(CuratorFramework client, String path, String newValue) throws Exception {
        if (client == null) return;

        if (getNode(client, path) == null) {
            // createNode(client, path, newValue, CreateMode.PERSISTENT);
            createNodeRecursively(client, path, newValue.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT);
        } else {
            byte[] bytes = newValue.getBytes(StandardCharsets.UTF_8);
            client.setData().forPath(path, bytes);
        }
    }

    public static void deleteNode(CuratorFramework client, String path) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            // 递归删除目录及其子节点
            client.delete().deletingChildrenIfNeeded().forPath(path);
            log.info("删除 {} 及其子节点成功!", path);
        }
    }

    public static String getNode(CuratorFramework client, String path) throws Exception {
        if (client == null) return null;
        byte[] bytes = client.getData().forPath(path);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static boolean existNode(CuratorFramework client, String path) throws Exception {
        return client != null && (client.checkExists().forPath(path) != null);
    }

}
