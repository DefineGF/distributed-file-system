package com.ksyun.campus.zookeeper.watcher;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

@FunctionalInterface
public interface ServerWatcherHandle {
    void handleEvent(PathChildrenCacheEvent event);
}
