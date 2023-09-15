package com.ksyun.campus.zookeeper.watcher;

import com.ksyun.campus.zookeeper.ZookeeperUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

/**
 * 用以监听 DataServer 数据的变更：
 * 1. 节点增删变更
 * 2. 节点内容变更
 */
@Slf4j
public abstract class ServerWatcher {
    protected final CuratorFramework client;

    public ServerWatcher(String zkInfo) {
        client = ZookeeperUtil.getCuratorFramework(zkInfo);
    }

    public void watchRootChildChanged(ServerWatcherHandle serverWatcherHandle) throws Exception {
        String rootPath = getRootPath();
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, rootPath, true);
        pathChildrenCache.getListenable().addListener((client, event) -> {
            serverWatcherHandle.handleEvent(event);
        });
        pathChildrenCache.start();
    }

    public abstract String getRootPath();
}

