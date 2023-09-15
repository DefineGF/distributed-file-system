package com.ksyun.campus.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;

import java.util.List;
import java.util.Objects;

public class DSZKConnector extends ServerRegister<DataServerMsg> {
    public final static String DATA_SERVER_ROOT_PATH = "/zk-ds";
    public final static String DS_PATH = "/ds";

    public DSZKConnector(String zkInfo) {
        super(zkInfo);
    }

    @Override
    public String generateServerPath(DataServerMsg dataServerMsg) {
        return generateDataServerPath(dataServerMsg);
    }


    public String registerDataServer(DataServerMsg msg) throws Exception {
        return registerServer(DATA_SERVER_ROOT_PATH, msg);
    }

    public List<DataServerMsg> getDataServerList() throws Exception {
        return getServerList(DATA_SERVER_ROOT_PATH, new TypeReference<DataServerMsg>() {});
    }

    public DataServerMsg getDataServer(String nodePath) throws Exception {
        return this.getServer(nodePath, new TypeReference<DataServerMsg>() {});
    }

    public void updateDataServer(DataServerMsg msg) throws Exception {
        updateServer(msg);
    }


    public static String generateDataServerPath(DataServerMsg msg) {
        String nodePath = msg.getNodeId();
        if (nodePath == null || "".equals(nodePath)) {
            nodePath = DATA_SERVER_ROOT_PATH + DS_PATH + Objects.hash(msg.getHost(), msg.getPort());
            msg.setNodeId(nodePath);
        }
        return nodePath;
    }
}
