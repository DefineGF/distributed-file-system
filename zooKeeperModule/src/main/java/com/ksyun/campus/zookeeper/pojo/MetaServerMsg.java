package com.ksyun.campus.zookeeper.pojo;

/**
 * MetaServer 在 zookeeper 中注册的信息
 */
public class MetaServerMsg {
    private String nodeId;
    private String host;
    private int port;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public MetaServerMsg() {}


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "MetaServerMsg{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
