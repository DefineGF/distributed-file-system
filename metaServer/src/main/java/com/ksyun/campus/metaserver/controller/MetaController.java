package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.domain.FileNode;
import com.ksyun.campus.metaserver.services.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

@RestController("/")
public class MetaController {
    private final Logger LOGGER = LoggerFactory.getLogger(MetaController.class);

    @Resource
    private MetaService metaService;

    @RequestMapping("stats")
    public ResponseEntity<?> stats(@RequestHeader(required = false) String fileSystem,
                                   @RequestParam String path) {
        StatInfo statInfo = metaService.stats(path);
        if (statInfo == null) {
            return new ResponseEntity<>("文件不存在", HttpStatus.BAD_REQUEST);
        } else {
            return new ResponseEntity<>(statInfo, HttpStatus.OK);
        }
    }

    @RequestMapping("create")
    public ResponseEntity<?> createFile(@RequestHeader(required = false) String fileSystem,
                                        @RequestParam String path) {
        FileNode fileNode = metaService.getFile(path);
        if (fileNode != null) {
            LOGGER.info("文件 {} 已存在", path);
            StatInfo statInfo = StatInfo.fromStatInfo(fileNode);
            return new ResponseEntity<>(statInfo, HttpStatus.OK);
        }

        // 文件不存在
        StatInfo statInfo = metaService.createFile(fileSystem, path);
        if (statInfo == null) {
            LOGGER.error("文件 {} 创建失败", path);
            return new ResponseEntity<>("文件创建失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            return new ResponseEntity<>(statInfo, HttpStatus.OK);
        }
    }

    @RequestMapping("mkdir")
    public ResponseEntity<StatInfo> mkdir(@RequestHeader(required = false) String fileSystem,
                                          @RequestParam String path) {
        FileNode dirInfo = metaService.getDir(path);
        if (dirInfo != null) {
            return new ResponseEntity<>(StatInfo.fromStatInfo(dirInfo), HttpStatus.OK);
        }
        StatInfo statInfo = metaService.mkdir(path, fileSystem);
        if (statInfo == null) {
            return new ResponseEntity<>(new StatInfo(), HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            return new ResponseEntity<>(statInfo, HttpStatus.OK);
        }
    }

    @RequestMapping("listdir")
    public ResponseEntity<?> listdir(@RequestHeader(required = false) String fileSystem,
                                          @RequestParam String path) {
        List<StatInfo> statInfoList = metaService.listDirectory(path);
        if (statInfoList == null) {
            return new ResponseEntity<>("文件夹不存在", HttpStatus.BAD_REQUEST);
        } else {
            return new ResponseEntity<>(statInfoList, HttpStatus.OK);
        }
    }

    /**
     * delete file or dir
     */
    @RequestMapping("delete")
    public ResponseEntity<Boolean> delete(@RequestHeader(required = false) String fileSystem,
                                    @RequestParam String path) {
        boolean result = metaService.delete(path, fileSystem);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     */
    @RequestMapping("write")
    public ResponseEntity<String> commitWrite(@RequestHeader(required = false) String fileSystem,
                                              @RequestBody byte[] content, @RequestParam String path,
                                              @RequestParam int offset, @RequestParam int length) {
        FileNode file = metaService.getFile(path);
        if (file == null) {
            StatInfo statInfo = metaService.createFile(fileSystem, path);
            if (statInfo == null) {
                LOGGER.warn("文件 {} 不存在, 且创建失败!", path);
                return new ResponseEntity<>("文件不存在，且创建失败", HttpStatus.BAD_REQUEST);
            }
            file = metaService.getFile(path);
        }
        metaService.write(fileSystem, path, content, offset, length, file);
        return new ResponseEntity<>("写入完毕", HttpStatus.OK);
    }

    /**
     * 当 client 需要读取数据时，向 client 返回负载均衡后的 ReplicaData 数据，过程如下：
     * 1. client 调用 /open 获取 FileStatsDTO, 即获取文件的 ReplicaDataList
     * 2. 从 ReplicaDataList 选择第一个 ReplicaData 并访问：
     *      - 成功，则获取数据，终止!
     *      - 失败，则重新访问 /open 接口，获取调整过顺序的 ReplicaDataList
     */
    @RequestMapping("open")
    public ResponseEntity<StatInfo> open(@RequestHeader(required = false) String fileSystem,
                                         @RequestParam String path) {
        StatInfo statInfo = metaService.stats(path);
        if (statInfo == null) {
            return new ResponseEntity<>(new StatInfo(), HttpStatus.BAD_REQUEST);
        } else {
            // 通过负载均衡算法对 FileStatsDTO 中 data-server 副本重新排序
            statInfo = metaService.open(statInfo);
            if (statInfo != null) {
                return new ResponseEntity<>(statInfo, HttpStatus.OK);
            } else {
                // 无可用的 DataServer
                LOGGER.warn("无可用的 data-server");
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }

}
