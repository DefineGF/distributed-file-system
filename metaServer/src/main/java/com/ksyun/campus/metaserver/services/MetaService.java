package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.MetaServerApplication;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.services.filesystem.ZKFileSystem;
import com.ksyun.campus.metaserver.services.loadbalance.DataServerManager;
import com.ksyun.campus.metaserver.services.filesystem.FileSystemImage;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.FileNode;
import com.ksyun.campus.zookeeper.ZookeeperUtil;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class MetaService {
    private final Logger LOGGER = LoggerFactory.getLogger(MetaService.class);

    private final static int TOTAL_TRY_TIME = 3;      // 重试次数
    private final static int TOTAL_REPLICA_COUNT = 3; // 需要写的副本数

    @Resource
    private DataServerManager dataServerManager;

    @Resource
    public FileSystemImage fileSystemImage;

    @Resource
    public ZKFileSystem zkFileSystem;

    @Resource
    private RestTemplate restTemplate;



    public StatInfo stats(String path) {
        path = FileSystemImage.rectifyPath(path);
        FileNode fileNode = fileSystemImage.getFile(path); // 先判断是文件类型
        if (fileNode == null) {
            fileNode = fileSystemImage.getDir(path);       // 再判断是目录类型
        }
        if (fileNode == null) return null;
        return StatInfo.fromStatInfo(fileNode);
    }

    public FileNode getFile(String path) {
        path = FileSystemImage.rectifyPath(path);
        return fileSystemImage.getFile(path);
    }

    public FileNode getDir(String path) {
        path = FileSystemImage.rectifyPath(path);
        return fileSystemImage.getDir(path);
    }


    public boolean existDir(String path) {
        path = FileSystemImage.rectifyPath(path);
        return fileSystemImage.getDir(path) != null;
    }

    /**
     * 创建文件过程：
     * 1. 选择合适的 data_server 副本，并写入;
     * 2. 写入 data_server:
     *      - 写入成功, 使用 FileSystemImage 创建相关元数据;
     *      - 写入失败, 重新选择合适的副本重试;
     * 3. 记录写入成功的副本信息，并返回给 client
     */
    public StatInfo createFile(String fileSystem, String path) {
        path = FileSystemImage.rectifyPath(path);

        String formatPath = FileSystemImage.getFormatPath(path); // 获取映射之后的文件路径(即保存在DataServer中的最终路径)
        List<ReplicaData> replicaDataList = getWritableReplicaDataList(fileSystem, formatPath);

        if (replicaDataList.size() == 0) {
            LOGGER.warn("向DataServer创建文件失败, 文件副本数为 0");
            return null;
        }
        // 成功写入 data-server 后, 在meta-server写入元数据
        FileNode fileNode = fileSystemImage.createFile(path);
        if (fileNode != null) {
            fileNode.setReplicaDataList(replicaDataList); // 添加文件副本
            dataServerManager.addNewFileUpdate(replicaDataList);
            LOGGER.info("{} 添加data-server 副本信息: {}", fileNode.getPath(), replicaDataList.size());

            // 在 zk 创建节点
            StatInfo statInfo = StatInfo.fromStatInfo(fileNode);
            try {
                zkFileSystem.createFile("/" + path, statInfo);
            } catch (Exception e) {
                LOGGER.error("zk 中创建文件失败: {}", e.getMessage(), e);
            }
            return statInfo;
        } else {
            LOGGER.warn("MetaServer 创建文件元数据失败!");
            return null;
        }
    }

    /**
     * 从可用的 DataServer 中获取指定 TOTAL_REPLICA_COUNT 数目的 DataServer 副本
     * 达不到要求时重试上限为 TOTAL_TRY_TIME
     */
    private List<ReplicaData> getWritableReplicaDataList(String fileSystem, String path) {
        List<ReplicaData> replicaDataList  = new LinkedList<>();
        Set<DataServerMsg> dataServerMsgSet = new HashSet<>(); // 已经选择过的 DataServer
        int tryTime = 0;
        int count = TOTAL_REPLICA_COUNT;
        while (++tryTime <= TOTAL_TRY_TIME) {
            List<DataServerMsg> writeDataServer = dataServerManager.getNextWritableList(count, 0); // 获取可用的 DataServer 列表
            LOGGER.info("负载均衡获得可用 DataServer 列表: ");
            for (DataServerMsg msg : writeDataServer) {
                LOGGER.info("\t" + msg);
            }
            for (DataServerMsg msg : writeDataServer) {
                // 从可用的 DataServer 中选择合适的
                if (dataServerMsgSet.contains(msg)) {
                    continue;
                }
                dataServerMsgSet.add(msg); // 记录
                String ipAndPort = msg.getHost() + ":" + msg.getPort();
                ReplicaData replicaData = conToDataServer(fileSystem, path, new byte[] {0}, 0, 0, ipAndPort); // 创建文件
                if (replicaData != null) {
                    replicaDataList.add(replicaData);
                    count--;
                }
            }
            if (count == 0) {
                // 达到一定的副本数，则中断重试过程
                break;
            }
        }
        return replicaDataList;
    }



    public StatInfo mkdir(String path, String fileSystem) {
        path = FileSystemImage.rectifyPath(path);
        FileNode fileNode = fileSystemImage.mkdir(path);
        if (fileNode != null) {
            StatInfo statInfo = StatInfo.fromStatInfo(fileNode);
            try {
                zkFileSystem.createDirectory("/" + path, statInfo);
            } catch (Exception e) {
                LOGGER.error("zk 中创建文件夹失败: {}", e.getMessage(), e);
            }
            return statInfo;
        } else {
            LOGGER.warn("MetaServer 创建文件夹 元数据失败!");
            return null;
        }
    }

    public List<StatInfo> listDirectory(String path) {
        path = FileSystemImage.rectifyPath(path);
        if (!existDir(path)) {
            return null;
        }

        FileNode fileNode = fileSystemImage.getDir(path);
        List<FileNode> children = fileNode.getChildren();
        if (children == null) {
            return null;
        }

        List<StatInfo> statInfoList = new LinkedList<>();
        for (FileNode info : children) {
            statInfoList.add(StatInfo.fromStatInfo(info));
        }
        return statInfoList;
    }


    /**
     * 删除文件或文件夹
     */
    public boolean delete(String path, String fileSystem) {
        path = FileSystemImage.rectifyPath(path);

        if (fileSystemImage.getFile(path) != null) {
            fileSystemImage.deleteFile(path);
            try {
                zkFileSystem.delete("/" + path);
            } catch (Exception e) {
                LOGGER.error("zk 删除文件失败: {}", e.getMessage(), e);
            }
            return true;
        } else if (fileSystemImage.getDir(path) != null) {
            fileSystemImage.deleteDir(path);
            try {
                zkFileSystem.delete("/" + path);
            } catch (Exception e) {
                LOGGER.error("zk 删除文件夹失败: {}", e.getMessage(), e);
            }
            return true;
        }
        return false;
    }

    /**
     * 写文件过程：
     * 1. 首先通过StatInfo 获取文件几个副本存放的位置；
     * 2. 将文件内容依次写入副本中；
     * 3. 记录写入结果
     */
    public void write(String fileSystem, String path, byte[] content, int offset, int length, FileNode fileNode) {
        int writeLen = Math.min(length, content.length);
        path = FileSystemImage.rectifyPath(path);

        String formatPath = FileSystemImage.getFormatPath(path);
        List<ReplicaData> replicaDataList = fileNode.getReplicaDataList();
        // 有一说一，这里可以使用多线程
        boolean flag = false;
        for (ReplicaData replicaData : replicaDataList) {
            ReplicaData ans = conToDataServer(fileSystem, formatPath, content, offset, length, replicaData.getDsNode());
            if (ans != null) {
                flag = true;
                LOGGER.info("文件 {} 写入 {} {} 字节", formatPath, replicaData.getId(), writeLen);
                fileNode.mtime = System.currentTimeMillis(); // 更新元数据修改时间
            }
        }
        if (flag) {
            // 有 DataServer 写入成功
            fileNode.size += writeLen;
            try {
                zkFileSystem.update("/" + path, StatInfo.fromStatInfo(fileNode));
            } catch (Exception e) {
                LOGGER.error("zk 更新文件 {} 失败: {}", path, e.getMessage(), e);
            }
        }
    }

    /**
     * 根据负载均衡算法，调整 ReplicaData 副本顺序
     */
    public StatInfo open(StatInfo statsDTO) {
        List<ReplicaData> replicaData = statsDTO.getReplicaData();
        ReplicaData next = dataServerManager.getNextReadableList(replicaData);
        if (next != null) {
            statsDTO.setReplicaData(replicaData);
            return statsDTO;
        }
        return null;
    }


    /**
     * 使用 RestTemplate 调用 data-server 中 write 接口, 获取可写副本
     */
    private ReplicaData conToDataServer(String fileSystem, String path, byte[] content, int offset, int length, String ipAndPort) {
        String targetURI = MetaServerApplication.PREFIX + ipAndPort + "/write?path=" + path + "&offset=" + offset + "&length=" + length;
        HttpHeaders headers = new HttpHeaders();
        headers.set("fileSystem", fileSystem);
        // 向接口 post 数据
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        ResponseEntity<ReplicaData> entity = restTemplate.exchange(targetURI,
                HttpMethod.POST, new HttpEntity<>(content, headers), ReplicaData.class);

        if (entity.getStatusCode() == HttpStatus.OK) {
            ReplicaData body = entity.getBody();
            LOGGER.info("写入成功, 获取内容: {}", body);
            return body;
        } else {
            LOGGER.warn("写入失败");
            return null;
        }
    }
}
