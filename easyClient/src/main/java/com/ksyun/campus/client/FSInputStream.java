package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.exception.DataFormatException;
import com.ksyun.campus.client.util.buffer.CircleDataBuffer;
import com.ksyun.campus.client.util.connector.DataServerConnector;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class FSInputStream extends InputStream {

    private final static Logger LOGGER = LoggerFactory.getLogger(FSInputStream.class);

    private final static int DATA_BUFFER_DEFAULT_SIZE = 1024;
    private int offset = 0; // 记录读取文件的偏移量
    private final CircleDataBuffer dataBuffer;
    private final DataServerConnector dataServerConnector; // 连接 data-server 用于读数据


    public FSInputStream(StatInfo statsDTO, EFileSystem eFileSystem) {
        this(statsDTO, eFileSystem, DATA_BUFFER_DEFAULT_SIZE);
    }

    public FSInputStream(StatInfo statsDTO, EFileSystem eFileSystem, int dataBufferSize) {
        this.dataServerConnector = new DataServerConnector(statsDTO, eFileSystem);
        this.dataBuffer = new CircleDataBuffer(dataBufferSize);
    }


    @Override
    public int read() throws IOException, DataFormatException {
        byte[] intByteArr = new byte[4];
        int len = read(intByteArr);
        if (len == 4) {
            return CircleDataBuffer.byteArrayToInt(intByteArr);
        } else {
            throw new DataFormatException("数据格式不匹配!");
        }
    }

    /**
     * 首先从缓冲区中获取数据：
     * - 如果 buffer 为空，先从 data-server 获取数据到 buffer
     * - 如果 buffer 数据足够，则直接读取；
     * - 如果 buffer 不够，则首先将缓冲区数据读完，再从 data-server中读取数据, 然后放入目标 byte[] 中，
     *   剩余的保存在 buffer 中
     */
    @Override
    public int read(byte[] b) throws IOException {
        if (dataBuffer.isEmpty()) {
            byte[] temp = new byte[dataBuffer.capacity()];
            int len = read(temp, offset, temp.length);
            if (len <= 0) {
                return -1;
            }
            offset += len;
            dataBuffer.copyFrom(temp, len); // 将 temp 内容拷贝到 dataBuffer
        }

        if (b.length <= dataBuffer.size()) {
            // 从 buffer 中读取数据
            return dataBuffer.read(b);
        } else {
            int len = dataBuffer.read(b);// 将缓冲区数据全部读出
            while (len < b.length) {
                byte[] temp = new byte[dataBuffer.capacity()];
                int ans = read(temp, offset, temp.length); //  从 data-server 中获取数据
                if (ans == -1) {
                    // 没有可以读取的了
                    return len;
                }
                offset += ans; // 记录文件偏移
                int nextLen = b.length - len; // 写满 b 还需要的数据量
                nextLen = Math.min(temp.length, nextLen);
                System.arraycopy(temp, 0, b, len, nextLen);
                len += nextLen;
                if (nextLen < temp.length) {
                    // temp 中还有没有写完的数据，写入 buffer 中
                    byte[] remain = new byte[temp.length - nextLen];
                    System.arraycopy(temp, nextLen, remain, 0, remain.length);
                    dataBuffer.write(remain);
                }
                // nextLen == temp.length: 说明读取的数据要么刚好满足 b 要求，要么还不够
            }
            return b.length;
        }
    }

    /**
     * 利用 DataServerConnector 直接从 DataServer 中读取数据
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        len = Math.min(b.length, len);
        byte[] ans = dataServerConnector.readFromDataServer(off, len);
        if (ans != null) {
            System.arraycopy(ans, 0, b, 0, ans.length);
            return ans.length;
        } else {
            return -1;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
