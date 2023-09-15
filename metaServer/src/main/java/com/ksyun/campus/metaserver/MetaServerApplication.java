package com.ksyun.campus.metaserver;

import com.ksyun.campus.metaserver.services.filesystem.ZKFileSystem;
import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.MSZKConnector;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class MetaServerApplication {
    public final static String PREFIX = "http://";

    public final static String IP = "localhost";

    @Value("${server.port}")
    public int port;

    @Value("${zookeeper.addr}")
    String zookeeperAddr;

    @Bean
    public MetaServerMsg metaServerMsg() {
        MetaServerMsg metaServerMsg = new MetaServerMsg();
        metaServerMsg.setHost(IP);
        metaServerMsg.setPort(port);
        MSZKConnector.generateMetaServerPath(metaServerMsg);
        return metaServerMsg;
    }

    @Bean
    public List<MetaServerMsg> metaServerMsgList() {
        return Collections.synchronizedList(new LinkedList<>());
    }

    // 用于注册 MetaServer
    @Bean
    public MSZKConnector msZKConnector() {
        return new MSZKConnector(zookeeperAddr);
    }

    // 用于获取 DataServer 信息
    @Bean
    public DSZKConnector dsZKConnector() {
        return new DSZKConnector(zookeeperAddr);
    }

    @Bean
    public ZKFileSystem zkFileSystem() {
        ZKFileSystem zkFileSystem = new ZKFileSystem(zookeeperAddr);
        zkFileSystem.flag = port + ": ";
        return zkFileSystem;
    }

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    public static void main(String[] args) {
        SpringApplication.run(MetaServerApplication.class,args);
    }

}
