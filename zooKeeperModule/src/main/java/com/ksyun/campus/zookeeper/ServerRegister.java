package com.ksyun.campus.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * 关于 data-server 和 meta-server 注册与修改
 */
@Slf4j
public abstract class ServerRegister<T> extends ZKConnector {

    public ServerRegister(String zkInfo) {
        super(zkInfo);
    }

    protected T getServer(String nodePath, TypeReference<T> reference) throws Exception {
        String content = ZookeeperUtil.getNode(client, nodePath);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(content, reference);
    }


    protected void updateServer(T serverMsg) throws Exception {
        String nodePath = generateServerPath(serverMsg);
        String content = new ObjectMapper().writeValueAsString(serverMsg);
        ZookeeperUtil.updateNode(client, nodePath, content);
    }

    protected String registerServer(String rootPath, T serverMsg) throws Exception {
        String serverNodePath = generateServerPath(serverMsg);

        if (ZookeeperUtil.existNode(client, serverNodePath)) {
            return serverNodePath;
        }

        if (!ZookeeperUtil.existNode(client, rootPath)) {
            // 根节点不存在则创建
            ZookeeperUtil.createNodeDirectly(client, rootPath, new byte[0], CreateMode.PERSISTENT);
        }

        String content = new ObjectMapper().writeValueAsString(serverMsg);
        String ans = ZookeeperUtil.createNodeDirectly(client, serverNodePath, content.getBytes(StandardCharsets.UTF_8), CreateMode.EPHEMERAL);
        log.info("zk 注册 {} 成功！", ans);
        return ans;
    }


    protected List<T> getServerList(String rootPath, TypeReference<T> typeReference) throws Exception {
        List<T> result = new LinkedList<>();
        List<String> children = client.getChildren().forPath(rootPath);
        for (String child : children) {
            T msg = getServer(rootPath + "/" + child, typeReference);
            result.add(msg);
        }
        return result;
    }

    public abstract String generateServerPath(T t);

}
