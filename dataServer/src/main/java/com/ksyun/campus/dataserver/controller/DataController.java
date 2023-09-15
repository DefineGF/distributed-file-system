package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.controller.dto.ReplicaData;
import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.File;

@RestController("/")
@Slf4j
public class DataController {
    private final  static Logger LOGGER = LoggerFactory.getLogger(DataController.class);

    @Value("${server.port}")
    public int port;

    @Resource
    private String jarWorkDirectory;

    @Resource
    private DataServerMsg dataServerMsg;

    @Resource
    private DataService dataService;

    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     */
    @PostMapping("write")
    public ResponseEntity<ReplicaData> writeFile(@RequestHeader(required = false) String fileSystem,
                                                 @RequestBody byte[] content,
                                                 @RequestParam String path,
                                                 @RequestParam int offset,
                                                 @RequestParam int length) {
        String serverId = dataServerMsg.getNodeId(); // /ds-xxx

        String targetPath = jarWorkDirectory + serverId +  "/" + path;
        targetPath = targetPath.replace('/', File.separatorChar);
        targetPath = targetPath.replace('\\', File.separatorChar);

        boolean ans = dataService.write(content, targetPath, offset, length); // 写入当前 data-server 实例
        if (ans) {
            ReplicaData replicaData = new ReplicaData();
            replicaData.id = serverId;
            replicaData.dsNode = "localhost:" + port;
            replicaData.path = targetPath;
            return new ResponseEntity<>(replicaData, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(new ReplicaData(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     */
    @RequestMapping("read")
    public ResponseEntity<byte[]> readFile(@RequestHeader(required = false) String fileSystem,
                                   @RequestParam String path,
                                   @RequestParam int offset,
                                   @RequestParam int length){
        String targetPath = jarWorkDirectory + "/" + dataServerMsg.getNodeId() +  "/" + path;
        byte[] ans = dataService.read(targetPath, offset, length);
        if (ans == null) {
            return new ResponseEntity<>(new byte[0], HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            return new ResponseEntity<>(ans, HttpStatus.OK);
        }
    }
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
