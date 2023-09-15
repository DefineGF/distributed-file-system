package com.ksyun.campus.zookeeper.watcher;

import com.ksyun.campus.zookeeper.DSZKConnector;

/**
 * Zookeeper 中 MetaServer 变化监听
 */
public class DataServerWatcher extends ServerWatcher {
    public DataServerWatcher(String zkInfo) {
        super(zkInfo);
    }

    @Override
    public String getRootPath() {
        return DSZKConnector.DATA_SERVER_ROOT_PATH;
    }
}