package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.services.loadbalance.DataServerManager;
import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.MSZKConnector;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import com.ksyun.campus.zookeeper.watcher.DataServerWatcher;
import com.ksyun.campus.zookeeper.watcher.MetaServerWatcher;
import com.ksyun.campus.zookeeper.watcher.ServerWatcherHandle;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
@Slf4j
public class RegistService implements ApplicationRunner {

    private final static Logger LOGGER = LoggerFactory.getLogger(RegistService.class);

    @Resource
    private DataServerManager dataServerManager; // 用于实现 DataServer 访问的负载均衡

    @Resource
    private MetaServerMsg metaServerMsg;

    @Resource
    private MSZKConnector msZKConnector;

    @Resource
    private DSZKConnector dsZKConnector;

    @Resource
    private List<MetaServerMsg> metaServerMsgList; // 全局 MetaServer 列表


    @Value("${zookeeper.addr}")
    String zookeeperAddr;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // 注册当前 MetaServer
        try {
            msZKConnector.registerMetaServer(metaServerMsg);
        } catch (Exception e) {
            LOGGER.error("MetaServer 注册失败: {}", e.getMessage(), e);
        }

        try {
            // 注册 DataServer 变化监听
            new DataServerWatcher(zookeeperAddr).watchRootChildChanged(dsWatcherHandler);
        } catch (Exception e) {
            LOGGER.error("添加 DataServer 监听失败: {}", e.getMessage(), e);
        }

        // 注册 MetaServer 变化监听
        try {
            new MetaServerWatcher(zookeeperAddr).watchRootChildChanged(msWatcherHandler);
        } catch (Exception e) {
            LOGGER.error("添加 MetaServer 监听失败: {}", e.getMessage(), e);
        }
        updateDataServerList();  // 获取当前 DataServer 信息
        updateMetaServerList();      // 获取当前 MetaServer 信息
    }

    // 处理 ZooKeeper 中 DataServer 变更的核心方法
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

    // 处理 Zookeeper 中 MetaServer 变更的核心方法
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
     * 更新 DataServer 列表
     */
    public void updateDataServerList() {
        try {
            List<DataServerMsg> dataServerList =  dsZKConnector.getDataServerList();
            dataServerManager.setAvailDataServerList(dataServerList);

            LOGGER.info("DataServer 可用列表更新: {}", dataServerList.size());
            for (DataServerMsg msg : dataServerList) {
                LOGGER.info("\t{}", msg);
            }
        } catch (Exception e) {
            LOGGER.error("DataServer 获取失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 从 ZooKeeper 更新 MetaServer 信息
     */
    public void updateMetaServerList() {
        try {
            List<MetaServerMsg> msgList = msZKConnector.getMetaServerList();
            metaServerMsgList.clear();
            metaServerMsgList.addAll(msgList);

            LOGGER.info("MetaServer 列表: " + msgList.size());
            for (MetaServerMsg msg : msgList) {
                LOGGER.info("\t{}", msg);
            }
        } catch (Exception e) {
            LOGGER.error("zookeeper error: 获取 MetaServer 失败, {}", e.getMessage(), e);
        }
    }
}
