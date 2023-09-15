package com.ksyun.campus.metaserver;

import com.ksyun.campus.zookeeper.ZookeeperUtil;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

public class FileSystemImageTest {
    String zkAddr = "127.0.0.1:9000,127.0.0.1:9001";

    @Test
    public void cleanZK() {
        CuratorFramework client = ZookeeperUtil.getCuratorFramework(zkAddr);
        String[] roots = {"/zk-ms", "/zk-ds", "/zk-fs"};
        for (String root : roots) {
            try {
                ZookeeperUtil.deleteNode(client, root);
            } catch (Exception e) {
                System.out.println("删除 " + root + " 失败: " + e.getMessage());
            }
        }
    }

//    @Test
//    public void testZK() {
//        CuratorFramework client = ZookeeperUtil.getCuratorFramework(zkAddr);
//        List<String> children = null;
//        try {
//            children = client.getChildren().forPath(ZKFileSystem.ROOT);
//            for (String child : children) {
//                String val = ZookeeperUtil.getNode(client, ZKFileSystem.ROOT + "/" + child);
//                System.out.println("key = " + child + " : " + val);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

    @Test
    public void testMkDir() {

    }
//    FileSystemImage fileSystemImage = new FileSystemImage();

//    void createDir() {
//        String[] dirs = {"dir1", "dir1/dir2", "dir1_1/dir2_2", "dir1/dir2_3"};
//        for (String dir : dirs) {
//            fileSystemImage.mkdir(dir);
//        }
//    }
//
//    void createFile() {
//        String[] files = {"readme.txt", "dir1/readme.txt", "dir1_1/readme.txt",
//                "dir1/dir2/readme.txt", "dir1/dir2/dir3/readme.txt"};
//        for (String file : files) {
//            fileSystemImage.createFile(file);
//        }
//    }
//
//    @Test
//    public void testCreate() {
//        createDir();
//        createFile();
//        fileSystemImage.logFileSystemInfo();
//    }
//
//
//    @Test
//    public void testGetDir() {
//        createDir();
//        createFile();
//        fileSystemImage.logFileSystemInfo();
//
//        String[] dirs = {"dir1/dir2/dir3", "dir1/dir2_3", "dir1/dir2_4", "dir2", "dir1_1", "dir2",
//                "dir1/dir2/dir3/", "dir1\\dir2_3", "/dir1\\dir2_4\\", "/dir2", "\\dir1_1", "/dir2"};
//
//        for (String dir : dirs) {
//            System.out.println(FileSystemImage.rectifyPath(dir));
//        }
//    }
//
//    @Test
//    public void testGetFile() {
//        createDir();
//        createFile();
//        fileSystemImage.logFileSystemInfo();
//
//        String[] files = {"dir1/readme.txt", "readme.txt", "dir/readme.txt",
//                          "dir1\\readme.txt", "dir1_1\\dir2_2\\readme.txt", "dir1_1\\readme.txt"};
//        for (String file : files) {
//            System.out.println(fileSystemImage.getFile(file));
//        }
//    }
//
//    @Test
//    public void testDeleteFile() {
//        createDir();
//        createFile();
//        fileSystemImage.logFileSystemInfo();
//
//        String[] files = {"dir1/dir2/dir3/readme.txt", "dir1/dir2/readme.txt",
//                "dir1_1/readme.txt", "readme.txt", "dir1/dir2/readme.txt"};
//        for (String file : files) {
//            System.out.println("删除文件: " + file);
//            fileSystemImage.deleteFile(file);
//            fileSystemImage.logFileSystemInfo();
//        }
//    }
//
//    @Test
//    public void testDeleteDir() {
//        createDir();
//        createFile();
//        fileSystemImage.logFileSystemInfo();
//
//        String[] dirs = {"dir1/dir2_3", "dir1_1", "dir1/dir2/"};
//        for (String dir : dirs) {
//            fileSystemImage.deleteDir(dir);
//            System.out.println("删除目录: " + dir);
//            fileSystemImage.logFileSystemInfo();
//        }
//    }
}
