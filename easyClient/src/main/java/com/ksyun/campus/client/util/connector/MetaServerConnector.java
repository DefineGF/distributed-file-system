package com.ksyun.campus.client.util.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.util.http.HttpClientUtil;
import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 完成与 meta-server 交互，主要用于创建文件、获取文件信息等功能
 */
@Slf4j
public class MetaServerConnector {
    private final static Logger LOGGER = LoggerFactory.getLogger(MetaServerConnector.class);
    private final EFileSystem eFileSystem;
    private final HttpClient httpClient;


    public MetaServerConnector(EFileSystem eFileSystem) {
        this.eFileSystem = eFileSystem;
        this.httpClient = HttpClientUtil.getHttpClient();
    }

    /**
     * 创建文件，并获取文件信息:
     * 1. 首先尝试在 MasterServer 中创建
     * 2. 成功的话，同步写入 副MetaServer;
     * 3. 失败则尝试 副MetaServer;
     */
    public StatInfo createFileToMetaServer(String path) {
        StatInfo result = tryCreateFileToMS(path, eFileSystem.getClusterInfo().getMasterMetaServer());
        if (result != null) {
            LOGGER.info("MasterMetaServer 获取结果: {}", result);
            return result;
        } else {
            LOGGER.info("MasterMetaServer 获取失败!");
        }

        // 尝试 副MetaServer
        result = tryCreateFileToMS(path, eFileSystem.getClusterInfo().getSlaveMetaServer());
        if (result != null) {
            LOGGER.info("SlaveMetaServer 获取结果: {}", result);
        } else {
            LOGGER.info("SlaveMetaServer 获取失败");
        }
        return result;
    }

    private StatInfo tryCreateFileToMS(String path, MetaServerMsg metaServerMsg) {
        if (metaServerMsg == null) {
            return null;
        }
        // 调用 meta-server 中 create 接口，获取文件分区信息
        String targetURI = EFileSystem.PREFIX + metaServerMsg.getHost() + ":" + metaServerMsg.getPort() + "/create?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        try {
            return httpClient.execute(httpGet, new CreateResponseHandler());
        } catch (IOException e) {
            LOGGER.error("createFile {} to MetaServer 失败: {}", path, e.getMessage(), e);
        }
        return null;
    }


    /**
     * 从 meta-serve 中读取文件信息
     */
    public StatInfo openFileFromMetaServer(String path) {
        StatInfo result;
        // 尝试从 MasterMetaServer 获取信息
        MetaServerMsg metaServer = eFileSystem.getClusterInfo().getMasterMetaServer();
        result = tryOpenFileFromMS(path,  metaServer);
        if (result != null) {
            LOGGER.info("MasterMS 获取数据: {}", result);
            return result;
        }

        // 尝试从 SlaveMetaServer 获取信息
        metaServer = eFileSystem.getClusterInfo().getSlaveMetaServer();
        result = tryOpenFileFromMS(path,  metaServer);
        if (result != null) {
            LOGGER.info("SlaveMS 获取数据: {}", result);
        }
        return result;
    }

    private StatInfo tryOpenFileFromMS(String path, MetaServerMsg metaServer) {
        if (metaServer == null) {
            return null;
        }
        LOGGER.info("尝试从 MS = {}-{}:{} 中 OpenFile：", metaServer.getNodeId(), metaServer.getHost(), metaServer.getPort());
        String targetURI = EFileSystem.PREFIX + metaServer.getHost() + ":" + metaServer.getPort() + "/open?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        StatInfo statInfo = null;
        try {
            statInfo = httpClient.execute(httpGet, new OpenResponseHandler());
        } catch (IOException e) {
            LOGGER.error("open file from MetaServer error: {}", e.getMessage(), e);
        }
        return statInfo;
    }


    /**
     * 在本 MetaServer 创建文件夹元数据
     */
    public boolean mkdir(String path) {
        boolean result = tryToMkDir(path, eFileSystem.getClusterInfo().getMasterMetaServer());
        if (result) {
            LOGGER.info("master 创建成功!");
            return true;
        }
        result = tryToMkDir(path, eFileSystem.getClusterInfo().getSlaveMetaServer());
        if (result) {
            LOGGER.info("slave 创建成功!");
            return true;
        }
        return false;
    }

    /**
     * 调用 MetaServer 创建文件
     */
    private boolean tryToMkDir(String path, MetaServerMsg metaServer) {
        if (metaServer == null)
            return false;
        String targetURI = EFileSystem.PREFIX + metaServer.getHost() + ":" + metaServer.getPort() + "/mkdir?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        boolean ans = false;
        try {
            ans = httpClient.execute(httpGet, classicHttpResponse -> {
                int code = classicHttpResponse.getCode();
                if (code == 200 || code == 400) {
                    LOGGER.info("文件夹创建成功 或者 已存在!");
                    return true;
                } else if (code == 500) {
                    LOGGER.warn("open file: 服务器内部错误!");
                }
                return false;
            });
        } catch (IOException e) {
            LOGGER.error("open file from MetaServer error: {}", e.getMessage(), e);
        }
        return ans;

    }

    public boolean delete(String path) {
        MetaServerMsg metaServer = eFileSystem.getClusterInfo().getMasterMetaServer();

        boolean result = tryToDelete(path, metaServer);
        if (result) {
            LOGGER.info("master 删除成功!");
            return true;
        }
        result = tryToDelete(path, eFileSystem.getClusterInfo().getSlaveMetaServer());
        if (result) {
            LOGGER.info("slave 删除成功!");
            return true;
        }
        return false;
    }

    private boolean tryToDelete(String path, MetaServerMsg metaServer) {
        if (metaServer == null) {
            return false;
        }
        String targetURI = EFileSystem.PREFIX + metaServer.getHost() + ":" + metaServer.getPort() + "/delete?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        boolean ans = false;
        try {
            ans = httpClient.execute(httpGet, classicHttpResponse -> {
                int code = classicHttpResponse.getCode();
                return code == 200;
            });
        } catch (IOException e) {
            LOGGER.error("delete 异常: {}", e.getMessage(), e);
        }
        return ans;
    }

    public StatInfo stats(String path) {
        MetaServerMsg metaServer = eFileSystem.getClusterInfo().getMasterMetaServer();
        StatInfo result = tryStats(path, metaServer);
        if (result != null) {
            LOGGER.info("master 获取成功: {}", result);
            return result;
        }
        result = tryStats(path, eFileSystem.getClusterInfo().getSlaveMetaServer());
        if (result != null) {
            LOGGER.info("slave 获取成功: {}", result);
            return result;
        }
        return null;
    }

    private StatInfo tryStats(String path, MetaServerMsg metaServer) {
        if (metaServer == null) {
            return null;
        }
        String targetURI = EFileSystem.PREFIX + metaServer.getHost() + ":" + metaServer.getPort()
                + "/stats?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        StatInfo statInfo = null;
        try {
            statInfo = httpClient.execute(httpGet, classicHttpResponse -> {
                int code = classicHttpResponse.getCode();
                if (code == 200) {
                    HttpEntity entity = classicHttpResponse.getEntity();
                    String content = EntityUtils.toString(entity);
                    ObjectMapper objectMapper = new ObjectMapper();
                    return objectMapper.readValue(content, StatInfo.class);
                } else {
                    LOGGER.warn("文件 {} 不存在", path);
                }
                return null;
            });
        } catch (IOException e) {
            LOGGER.error("stats 执行出错: {}", e.getMessage(), e);
        }
        return  statInfo;
    }

    public List<StatInfo> listDir(String path) {
        MetaServerMsg metaServer = eFileSystem.getClusterInfo().getMasterMetaServer();
        List<StatInfo> result = tryListDir(path, metaServer);
        if (result != null) {
            LOGGER.info("master 获取成功!");
            return result;
        }
        result = tryListDir(path, eFileSystem.getClusterInfo().getSlaveMetaServer());
        if (result != null) {
            LOGGER.info("slave 获取成功");
            return result;
        }
        return null;
    }

    private List<StatInfo> tryListDir(String path, MetaServerMsg metaServer) {
        if (metaServer == null) {
            return null;
        }
        String targetURI = EFileSystem.PREFIX + metaServer.getHost() + ":" + metaServer.getPort() + "/listdir?path=" + path;
        HttpGet httpGet = new HttpGet(targetURI);
        httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        List<StatInfo> result = null;
        try {
            result = httpClient.execute(httpGet, classicHttpResponse -> {
                int code = classicHttpResponse.getCode();
                List<StatInfo> fileNodeList = new LinkedList<>();
                if (code == 200) {
                    HttpEntity entity = classicHttpResponse.getEntity();
                    String content = EntityUtils.toString(entity);
                    ObjectMapper objectMapper = new ObjectMapper();
                    List<StatInfo> statInfoList = objectMapper.readValue(content, new TypeReference<List<StatInfo>>(){});
                    fileNodeList.addAll(statInfoList);
                } else if (code == 400) {
                    LOGGER.warn("没有相关文件夹: {}", path);
                }
                return fileNodeList;
            });
        } catch (IOException e) {
            LOGGER.error("请求 listDir path = {} 失败! 错误信息: {}", path, e.getMessage(), e);
        }
        return result;
    }

    private static class CreateResponseHandler implements HttpClientResponseHandler<StatInfo> {
        @Override
        public StatInfo handleResponse(ClassicHttpResponse classicHttpResponse)
                throws HttpException, IOException {

            if (classicHttpResponse.getCode() == 200) {
                HttpEntity entity = classicHttpResponse.getEntity();
                String content = EntityUtils.toString(entity);

                ObjectMapper objectMapper = new ObjectMapper();
                StatInfo file = objectMapper.readValue(content, StatInfo.class);
                LOGGER.info("获取 MS create - {}, size={}B 创建响应: ", file.getPath(), file.getSize());
                for (ReplicaData data : file.getReplicaData()) {
                    LOGGER.info("\t副本信息: {}", data);
                }
                return file;
            } else if (classicHttpResponse.getCode() == 500) {
                LOGGER.warn("create_file 500, 服务器内部出错 500: {}", EntityUtils.toString(classicHttpResponse.getEntity()));
            }
            return null;
        }
    }

    private static class OpenResponseHandler implements HttpClientResponseHandler<StatInfo> {

        @Override
        public StatInfo handleResponse(ClassicHttpResponse classicHttpResponse) throws HttpException, IOException {
            int code = classicHttpResponse.getCode();
            if (code == 200) {
                String content = EntityUtils.toString(classicHttpResponse.getEntity());

                ObjectMapper objectMapper = new ObjectMapper();
                StatInfo file = objectMapper.readValue(content, StatInfo.class);
                LOGGER.info("open or mkdir successful: {}", content);
                return file;
            } else if (code == 400) {
                LOGGER.warn("open file: 文件不存在");
            } else if (code == 500) {
                LOGGER.warn("open file: 服务器内部错误!");
            }
            return null;
        }
    }


}
