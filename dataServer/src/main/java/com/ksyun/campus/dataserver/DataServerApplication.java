package com.ksyun.campus.dataserver;

import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;

@Slf4j
@SpringBootApplication
public class DataServerApplication {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataServerApplication.class);

    public final static String IP = "localhost";

    @Value("${server.port}")
    public int port;

    @Value("${zookeeper.addr}")
    String zookeeperAddr;

    @Resource
    private Environment environment;


    // 获取 jar 所在目录
    @Bean
    String jarWorkDirectory() {
        String dir = environment.getProperty("user.dir");
        LOGGER.info("springboot 获取目录: {}", dir);
        return dir;
    }

    @Bean
    public DataServerMsg dataServerMsg() {
        DataServerMsg dataServerMsg = new DataServerMsg();
        dataServerMsg.setHost(IP);
        dataServerMsg.setPort(port);
        DSZKConnector.generateDataServerPath(dataServerMsg); // 为 data-server 生成唯一 id
        dataServerMsg.setUseCapacity(0);
        dataServerMsg.setCapacity(200 * 1000 * 1000); // 200 M
        return dataServerMsg;
    }

    @Bean
    public DSZKConnector DSZKConnector() {
        return new DSZKConnector(zookeeperAddr);
    }


    public static void main(String[] args) {
        SpringApplication.run(DataServerApplication.class,args);
    }
}
