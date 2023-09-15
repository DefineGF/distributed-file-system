package com.ksyun.campus.metaserver.domain;

import lombok.Data;

import java.util.List;


@Data
public class StatInfo {
    public String path; //  文件元数据路径
    public long size;   // 文件大小
    public long mtime;  // 文件修改时间
    public FileType type;
    public List<ReplicaData> replicaData;

    public static StatInfo fromStatInfo(FileNode info) {
        StatInfo statInfo = new StatInfo();
        statInfo.path = info.getPath();
        statInfo.size = info.getSize();
        statInfo.mtime = info.getMtime();
        statInfo.type = info.getType();
        statInfo.replicaData = info.getReplicaDataList();
        return statInfo;
    }
}
