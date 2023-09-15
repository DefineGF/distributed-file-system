package com.ksyun.campus.client.domain;


import java.util.List;


public class StatInfo {
    public String path; //  文件元数据路径
    public long size;   // 文件大小
    public long mtime;  // 文件修改时间
    public FileType type;
    public List<ReplicaData> replicaData;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public FileType getType() {
        return type;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public List<ReplicaData> getReplicaData() {
        return replicaData;
    }

    public void setReplicaData(List<ReplicaData> replicaData) {
        this.replicaData = replicaData;
    }


    @Override
    public String toString() {
        return "StatInfo {" +
                "path='" + path + '\'' +
                ", size=" + size +
                ", mtime=" + mtime +
                ", type=" + type +
                ", replicaData=" + replicaData +
                '}';
    }
}
