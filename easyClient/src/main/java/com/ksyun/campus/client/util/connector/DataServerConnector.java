package com.ksyun.campus.client.util.connector;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.util.http.HttpClientUtil;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 连接 data-server，主要完成数据的读、写功能
 */
public class DataServerConnector {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataServerConnector.class);

    private final StatInfo statInfo;
    private final HttpClient httpClient;
    private final EFileSystem eFileSystem;

    public DataServerConnector(StatInfo statInfo, EFileSystem eFileSystem) {
        this.eFileSystem = eFileSystem;
        httpClient = HttpClientUtil.getHttpClient();
        this.statInfo = statInfo;
    }

    public boolean writeToAllDataServers(byte[] content, int offset, int length) {
        List<ReplicaData> replicaDataList = statInfo.getReplicaData();
        boolean ans = false;
        for (ReplicaData replicaData : replicaDataList) {
            boolean temp = writeToDataServer(replicaData, content, offset, length);
            ans = ans || temp;
        }
        return ans; // 但凡有一个 DataServer 写入副本成功，即算成功
    }

    private boolean writeToDataServer(ReplicaData replicaData, byte[] content, int offset, int length) {
        if (length < content.length) {
            byte[] copy = new byte[length];
            System.arraycopy(content, 0, copy, 0, length);
            content = copy;
        } else if (length > content.length) {
            length = content.length;
        }

        String targetURI = EFileSystem.PREFIX + replicaData.dsNode + "/write?path=" + getNameFromPath(replicaData.path) + "&offset=" + offset + "&length=" + length;
        HttpPost httpPost = new HttpPost(targetURI);
        httpPost.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM);
        ByteArrayEntity byteArrayEntity = new ByteArrayEntity(content, ContentType.APPLICATION_OCTET_STREAM);
        httpPost.setEntity(byteArrayEntity);

        boolean ans = false;
        try {
            ans = httpClient.execute(httpPost, new WriteResponseHandler());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("文件是否写入成功: " + ans);
        return ans;
    }


    /**
     * 从 DataServer 获取数据
     * @return: null or byte[].length > 0
     */
    public byte[] readFromDataServer(int offset, int length) {
        // 从任一 ReplicaData 中读取数据
        for (ReplicaData replicaData : statInfo.getReplicaData()) {
            String targetURI = EFileSystem.PREFIX + replicaData.dsNode
                    + "/read?path=" + getNameFromPath(replicaData.path) + "&offset=" + offset + "&length=" + length;
            HttpGet httpGet = new HttpGet(targetURI);
            httpGet.setHeader(EFileSystem.FILE_SYSTEM_KEY, eFileSystem.getFileName());
            httpGet.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_OCTET_STREAM);

            byte[] ans;
            try {
                ans = httpClient.execute(httpGet, new ReadResponseHandler());
                LOGGER.info("读取 offset = {}, length = {}, 结果 = {}", offset, length, Arrays.toString(ans));
                return ans;
            } catch (IOException e) {
                LOGGER.error("读取 {} 失败: {}", replicaData.id, e.getMessage(), e);
            }
        }
        return null;
    }


    private static class WriteResponseHandler implements HttpClientResponseHandler<Boolean> {

        @Override
        public Boolean handleResponse(ClassicHttpResponse classicHttpResponse) throws HttpException, IOException {
            int code =  classicHttpResponse.getCode();
            if (code == 200) {
                System.out.println("文件写入成功");
                return true;
            } else if (code == 500) {
                System.out.println("文件写入失败");
                return false;
            }
            return false;
        }
    }

    private static class ReadResponseHandler implements HttpClientResponseHandler<byte[]> {
        @Override
        public byte[] handleResponse(ClassicHttpResponse classicHttpResponse)
                throws HttpException, IOException {
            int code = classicHttpResponse.getCode();
            if (code == 200) {
                HttpEntity entity = classicHttpResponse.getEntity();
                return EntityUtils.toByteArray(entity);
            } else if (code == 500) {
                LOGGER.error("获取 byte[] 失败!");
            }
            return null;
        }
    }


    private String getNameFromPath(String path) {
        path = path.replace("\\", "/");
        int index = path.lastIndexOf("/");
        path = path.substring(index + 1);
        return path;
    }

}
