package com.ksyun.campus.zookeeper.pojo;

import lombok.Data;

import java.util.Objects;

/**
 * dataserver 在 zookeeper 注册的信息
 */
@Data
public class DataServerMsg {
    private String nodeId; // 存放在 zookeeper 下唯一节点id
    private String host;
    private int port;
    private int fileTotal = 0;
    private int capacity;
    private int useCapacity = 0;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;

        if (obj instanceof DataServerMsg) {
            DataServerMsg other = (DataServerMsg) obj;
            return this.nodeId.equals(other.getNodeId()) &&
                    this.host.equals(other.getHost()) &&
                    this.port == other.getPort();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.nodeId, this.host, this.port);
    }

    public void init(DataServerMsg other) {
        if (other == null) return;

        this.nodeId = other.getNodeId();
        this.host = other.getHost();
        this.port = other.getPort();
        this.fileTotal = other.getFileTotal();
        this.capacity = other.getCapacity();
        this.useCapacity = other.getUseCapacity();
    }

}
