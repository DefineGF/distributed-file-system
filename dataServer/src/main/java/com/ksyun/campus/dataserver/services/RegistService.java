package com.ksyun.campus.dataserver.services;


import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;

@Component
@Slf4j
public class RegistService implements ApplicationRunner {
    private final Logger LOGGER = LoggerFactory.getLogger(RegistService.class);

    private final List<DataServerMsg> DATA_SERVER_MSG_LIST = new LinkedList<>();     // zookeeper 中注册的实例

    @Resource
    private DataServerMsg dataServerMsg; // 当前 data-server 实例

    @Resource
    private DSZKConnector DSZKConnector;


    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }

    public void registToCenter() {
        try {
            DSZKConnector.registerDataServer(dataServerMsg);
        } catch (Exception e) {
            LOGGER.error("data-server 注册失败: {}", e.getMessage(), e);
        }

    }

    /**
     * 将 zookeeper 保存的 dataserver 数据同步到内存中的列表中
     */
    public void updateDataServerList(List<DataServerMsg> dataServerMsgs) {
        if (dataServerMsgs == null) return;

        DATA_SERVER_MSG_LIST.clear();
        DATA_SERVER_MSG_LIST.addAll(dataServerMsgs);
    }

    public List<DataServerMsg> getDataServerRegMsgList() {
        return DATA_SERVER_MSG_LIST;
    }

}
