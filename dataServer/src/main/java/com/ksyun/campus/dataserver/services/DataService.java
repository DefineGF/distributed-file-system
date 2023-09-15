package com.ksyun.campus.dataserver.services;

import com.ksyun.campus.zookeeper.DSZKConnector;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

@Service
@Slf4j
public class DataService {
    private final Logger LOGGER = LoggerFactory.getLogger(DataService.class);

    @Resource
    private DSZKConnector DSZKConnector;

    @Resource
    private DataServerMsg dataServerMsg;

    public boolean write(byte[] content, String path, int offset, int length) {
        File file = new File(path);
        boolean ans = true;
        if (!file.exists()) {
            File parentDir = new File(file.getParent());
            if (!parentDir.exists()) {
                // 创建 父目录
                ans = parentDir.mkdirs();
                if (!ans) {
                    LOGGER.warn("文件夹 {} 创建失败! ", parentDir.getPath());
                    return false; // 文件夹创建失败
                }
            }
            // 创建文件
            try {
                ans = file.createNewFile();
                if (ans) {
                    // 创建文件成功，则记录在 zookeeper
                    LOGGER.info("文件创建成功: {}", file.getAbsolutePath());
                    dataServerMsg.setFileTotal(dataServerMsg.getFileTotal() + 1);
                    try {
                        DSZKConnector.updateDataServer(dataServerMsg);
                    } catch (Exception e) {
                        LOGGER.error("更新 DataServer file_total 失败: {}", e.getMessage(), e);
                    }
                }
            } catch (IOException e) {
                LOGGER.error("文件 {} 创建失败!", path, e);
                return false;
            }
        }

        if (!ans) {
            return false;
        }
        if (length == 0) {
            return true; // 只是创建文件罢了
        }

        boolean writeIsSuc;
        try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
            raf.seek(offset);
            raf.write(content, 0, length);
            writeIsSuc = true;
        } catch (IOException e) {
            LOGGER.error("文件 {} 写入失败!", path, e);
            writeIsSuc = false;
        }

        if (writeIsSuc) {
            dataServerMsg.setUseCapacity(dataServerMsg.getUseCapacity() + length);
            try {
                DSZKConnector.updateDataServer(dataServerMsg);
            } catch (Exception e) {
                LOGGER.error("更新 DataServer use_capacity 失败: {}", e.getMessage(), e);
            }
        }
        return writeIsSuc;
    }

    public byte[] read(String path, int offset, int length){
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            raf.seek(offset);
            byte[] buffer = new byte[length];
            int count =  raf.read(buffer, 0, length);
            if (count == length) {
                return buffer;
            } else if (count != -1) {
                // 返回读取到的真正数据量
                byte[] temp = new byte[count];
                System.arraycopy(buffer, 0, temp, 0, count);
                return temp;
            }
        } catch (IOException e) {
            LOGGER.error("文件 {} 读取失败: {}", path, e.getMessage(), e);
        }
        return null;
    }
}
