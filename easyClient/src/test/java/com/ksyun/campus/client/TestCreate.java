package com.ksyun.campus.client;

import com.ksyun.campus.zookeeper.pojo.MetaServerMsg;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class TestCreate {
    EFileSystem eFileSystem = new EFileSystem();
    String[] ss = {"/dir1", "/dir1/dir2", "/dir1/test.txt"};

    @Test
    public void testCreate() {
        eFileSystem.mkdir(ss[0]);
        eFileSystem.mkdir(ss[1]);
        eFileSystem.create(ss[2]);
    }

    @Test
    public void testCreateFile() {
        String path = "/dir1/test.txt";
        eFileSystem.create(path);
    }

    @Test
    public void testSlave() {
        MetaServerMsg slaveMetaServer = eFileSystem.getClusterInfo().getSlaveMetaServer();

    }

    @Test
    public void testWrite() {
        FSOutputStream fsOutputStream = eFileSystem.create(ss[2]);
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 10000; ++i) {
            sb.append("醉后不知天在水, 满船清梦压星河").append(i).append("\n");
            if (i % 1000 == 0) {
                byte[] temps = sb.toString().getBytes(StandardCharsets.UTF_8);
                try {
                    fsOutputStream.write(temps);
                } catch (IOException e) {
                    System.out.println("写入出错: " + e.getMessage());
                    e.printStackTrace();
                }
                sb.setLength(0);
            }
        }
        try {
            fsOutputStream.flush();
            fsOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRead() {
        FSInputStream fis = eFileSystem.open(ss[2]);
        if (fis == null) {
            System.out.println("打开文件失败!");
            return;
        }
        byte[] ans = new byte[1024];
        try {
            int len = fis.read(ans);
            System.out.println("读取长度: " + len);

            if (len > 0) {
                byte[] content = new byte[len];
                System.arraycopy(ans, 0, content, 0, len);
                System.out.println("读取内容: " + new String(content, StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        boolean flag = eFileSystem.delete(ss[2]);
        System.out.println("删除成功：" + flag);
    }
}
