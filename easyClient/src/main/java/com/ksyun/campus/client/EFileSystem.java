package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.*;
import com.ksyun.campus.client.util.connector.MetaServerConnector;
import com.ksyun.campus.client.util.http.HttpClientUtil;
import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.MSZKConnector;
import com.ksyun.campus.zookeeper.watcher.DataServerWatcher;
import com.ksyun.campus.zookeeper.watcher.MetaServerWatcher;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import com.ksyun.campus.zookeeper.watcher.ServerWatcherHandle;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class EFileSystem extends FileSystem {
    private final static Logger LOGGER = LoggerFactory.getLogger(EFileSystem.class);
    private String zkInfo = "127.0.0.1:9000,127.0.0.1:9001"; // zookeeper 连接信息 127.0.0.1:9000,127.0.0.1:9001 10.0.0.201:2181

    public final static String PREFIX = "http://";
    public final static String FILE_SYSTEM_KEY = "fileSystem";

    private DSZKConnector dsZKConnector; // 获取 DataServer 信息
    private MSZKConnector msZKConnector; // 获取 MetaServer 信息

    private MetaServerConnector metaServerConnector; // 与 MetaServer 交互

    private final ClusterInfo clusterInfo;

    public EFileSystem() {
        this("");
    }

    public EFileSystem(String fileName) {
        this.fileSystem = fileName;
        clusterInfo = new ClusterInfo();  // 集群信息
        init();
    }

    public EFileSystem(String fileName, String zkInfo) {
        this.fileSystem = fileName;
        clusterInfo = new ClusterInfo();
        this.zkInfo = zkInfo;
        init();
    }

    private void init() {
        if (httpClient == null) {
            httpClient = HttpClientUtil.getHttpClient();
        }
        this.metaServerConnector = new MetaServerConnector(this);
        dsZKConnector = new DSZKConnector(zkInfo);
        msZKConnector = new MSZKConnector(zkInfo);

        updateDataServerList(); // 获取 data-server 列表
        updateMetaServerList();     // 获取 meta-server 列表

        try {
            new DataServerWatcher(zkInfo).watchRootChildChanged(dsWatcherHandler);
        } catch (Exception e) {
            LOGGER.error("Zookeeper 添加 DataServer 监听失败!");
        }

        try {
            new MetaServerWatcher(zkInfo).watchRootChildChanged(msWatcherHandler);
        } catch (Exception e) {
            LOGGER.error("Zookeeper 添加 MetaServer 监听失败!");
        }

    }

    ServerWatcherHandle dsWatcherHandler = event -> {
        if (event != null && event.getData() != null) {
            LOGGER.info("DS 事件: 类型 = {}, path = {}, data = {}", event.getType(), event.getData().getPath(), new String(event.getData().getData()));
            PathChildrenCacheEvent.Type type = event.getType();
            if (type.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
                    || type.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)
                    || type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                // 出现增加、删除、修改节点时, 更新可用列表
                updateDataServerList();
            }
        }
    };

    ServerWatcherHandle msWatcherHandler = event -> {
        if (event != null && event.getData() != null) {
            LOGGER.info("MS 事件: 类型 = {}, path = {}, data = {}", event.getType(), event.getData().getPath(), new String(event.getData().getData()));
            PathChildrenCacheEvent.Type type = event.getType();
            if (type.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
                    || type.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)
                    || type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                // 出现增加、删除、修改节点时, 更新可用列表
                updateMetaServerList();
            }
        }
    };

    /**
     * 读取
     */
    public FSInputStream open(String path){
        StatInfo statInfo = this.metaServerConnector.openFileFromMetaServer(path);
        if (statInfo == null) {
            LOGGER.warn("openFileFromMetaServer 获取失败");
            return null;
        }
        return new FSInputStream(statInfo, this);
    }

    /**
     * 写入文件
     */
    public FSOutputStream create(String path){
        StatInfo file = this.metaServerConnector.createFileToMetaServer(path);
        if (file == null) {
            LOGGER.warn("创建文件 {} 失败!", path);
            return null;
        }
        return new FSOutputStream(file, this);
    }


    public boolean mkdir(String path){
        return this.metaServerConnector.mkdir(path);
    }

    public boolean delete(String path){
        return this.metaServerConnector.delete(path);
    }

    public StatInfo getFileStats(String path){
        return this.metaServerConnector.stats(path);
    }

    public List<StatInfo> listFileStats(String path){
        return this.metaServerConnector.listDir(path);
    }

    public String getFileName() {
        return this.fileSystem;
    }

    public ClusterInfo getClusterInfo(){
        return clusterInfo;
    }

    /**
     * 从 Zookeeper 中获取 DataServer 信息并更新
     */
    public void updateDataServerList() {
        List<DataServerMsg> dataServerMsgList;
        try {
            dataServerMsgList = dsZKConnector.getDataServerList();
            clusterInfo.setDataServer(dataServerMsgList);
            LOGGER.info("DataServer 列表: ");
            for (DataServerMsg msg : dataServerMsgList) {
                LOGGER.info("\t{}", msg.toString());
            }
        } catch (Exception e) {
            LOGGER.error("zookeeper error: 获取 DataServer 失败, {}", e.getMessage(), e);
        }
    }

    /**
     * 从 ZooKeeper 更新 MetaServer 信息
     */
    public void updateMetaServerList() {
        try {
            List<MetaServerMsg> msgList = msZKConnector.getMetaServerList();
            LOGGER.info("MetaServer 列表: " + msgList.size());
            if (msgList.size() == 2) {
                // 先根据 port 排序
                msgList = msgList.stream()
                        .sorted(Comparator.comparing(MetaServerMsg::getPort))
                        .collect(Collectors.toList());
                clusterInfo.setMasterMetaServer(msgList.get(0));
                clusterInfo.setSlaveMetaServer(msgList.get(1));
            } else if (msgList.size() == 1) {
                clusterInfo.setMasterMetaServer(msgList.get(0));
                clusterInfo.setSlaveMetaServer(null);
            } else {
                clusterInfo.setMasterMetaServer(null);
                clusterInfo.setSlaveMetaServer(null);
            }
        } catch (Exception e) {
            LOGGER.error("zookeeper error: 获取 MetaServer 失败, {}", e.getMessage(), e);
        }
    }
}
