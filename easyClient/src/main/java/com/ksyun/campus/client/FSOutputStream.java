package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.buffer.CircleDataBuffer;
import com.ksyun.campus.client.util.connector.DataServerConnector;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;


@Slf4j
public class FSOutputStream extends OutputStream {
    private final static Logger LOGGER = LoggerFactory.getLogger(FSOutputStream.class);

    private final DataServerConnector dataServerConnector;
    private int offset = 0; // 当前写入文件的记录

    private final static int DATA_BUFFER_DEFAULT_SIZE = 1024;

    private final CircleDataBuffer buffer;

    public FSOutputStream(StatInfo statInfo, EFileSystem eFileSystem) {
        this(statInfo, eFileSystem, DATA_BUFFER_DEFAULT_SIZE);
    }

    public FSOutputStream(StatInfo statInfo, EFileSystem eFileSystem, int bufferSize) {
        this.dataServerConnector = new DataServerConnector(statInfo, eFileSystem);
        this.buffer = new CircleDataBuffer(bufferSize);
    }

    @Override
    public void write(int b) throws IOException {
        byte[] intBytes = CircleDataBuffer.intToByteArray(b);
        write(intBytes);
    }

    /**
     * 将数据通过 buffer 写入 data-server 中
     * 1. 如果写入数据大于 buffer 容量：先将buffer中数据写入data-server中，再将 数据直接写入 data-server;
     * 2. 否则，直接写入 buffer 中
     */
    @Override
    public void write(byte[] b) throws IOException {
        if (b == null || b.length == 0) return;

        if (b.length < buffer.freeSpace()) {
            // 直接写入buffer中即可
            buffer.write(b);
        } else {
            boolean ans;
            // 先将 buffer 数据直接写入 data-server 中
            if (!buffer.isEmpty()) {
                byte[] data = buffer.getData();
                ans = dataServerConnector.writeToAllDataServers(data, offset, data.length);
                offset += data.length;
                LOGGER.info("写入 buffer 结果: {}", ans);
            }
            // 然后将 b 中数据直接写入 data-server 中
            ans = dataServerConnector.writeToAllDataServers(b, offset, b.length);
            offset += b.length;
            LOGGER.info("写入 b 结果: {}", ans);
        }
    }

    @Override
    public void flush() throws IOException {
        if (!buffer.isEmpty()) {
            // 将缓冲区剩余内容写入
            byte[] data = buffer.getData();
            boolean ans = dataServerConnector.writeToAllDataServers(data, offset, data.length);
            if (ans) {
                offset += data.length;
                LOGGER.info("写入成功, 写入长度: {}", data.length);
            }
        }
    }



    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        boolean ans = dataServerConnector.writeToAllDataServers(b, offset, len);
        LOGGER.info("随机写结果: {}", ans);
    }


    @Override
    public void close() throws IOException {
        flush();
        super.close();
    }
}
