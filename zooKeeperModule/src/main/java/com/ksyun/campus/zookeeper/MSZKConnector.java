package com.ksyun.campus.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;

import java.util.List;
import java.util.Objects;

public class MSZKConnector extends ServerRegister<MetaServerMsg> {
    public final static String META_SERVER_ROOT_PATH = "/zk-ms";
    public final static String MS_PATH = "/ms";

    public MSZKConnector(String zkInfo) {
        super(zkInfo);
    }

    @Override
    public String generateServerPath(MetaServerMsg metaServerMsg) {
        return generateMetaServerPath(metaServerMsg);
    }

    /**
     * 注册 MetaServer 信息
     * @return 创建的 MetaServer 节点名
     */
    public String registerMetaServer(MetaServerMsg msg) throws Exception {
        return registerServer(META_SERVER_ROOT_PATH, msg);
    }

    public List<MetaServerMsg> getMetaServerList() throws Exception {
        return getServerList(META_SERVER_ROOT_PATH, new TypeReference<MetaServerMsg>() {});
    }

    public MetaServerMsg getMetaServer(String nodePath) throws Exception {
        return this.getServer(nodePath, new TypeReference<MetaServerMsg>() {});
    }

    public void updateMetaServer(MetaServerMsg msg) throws Exception {
        updateServer(msg);
    }

    public static String generateMetaServerPath(MetaServerMsg msg) {
        String nodePath = msg.getNodeId();
        if (nodePath == null || "".equals(nodePath)) {
            nodePath = META_SERVER_ROOT_PATH + MS_PATH + Objects.hash(msg.getHost(), msg.getPort());
            msg.setNodeId(nodePath);
        }
        return nodePath;
    }
}
