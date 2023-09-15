package com.ksyun.campus.zookeeper.watcher;

import com.ksyun.campus.zookeeper.MSZKConnector;

/**
 * Zookeeper 中 MetaServer 变化监听
 */
public class MetaServerWatcher extends ServerWatcher {
    public MetaServerWatcher(String zkInfo) {
        super(zkInfo);
    }

    @Override
    public String getRootPath() {
        return MSZKConnector.META_SERVER_ROOT_PATH;
    }
}
