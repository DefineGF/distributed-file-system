package com.ksyun.campus.zookeeper;

import org.apache.curator.framework.CuratorFramework;

public abstract class ZKConnector {
    protected final String zkConnectInfo;
    protected final CuratorFramework client;

    public ZKConnector(String zkInfo) {
        client = ZookeeperUtil.getCuratorFramework(zkInfo);
        this.zkConnectInfo = zkInfo;
    }

    public String getZkConnectInfo() {
        return zkConnectInfo;
    }
}
