package com.ksyun.campus.metaserver.domain;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class FileNode {
    public String path;
    public long size;
    public long mtime;
    public FileType type;

    private List<ReplicaData> replicaDataList;

    private List<FileNode> children;

    private FileNode parent;

    public FileNode() {}

    public static FileNode createDir(String path) {
        FileNode fileNode = new FileNode();
        fileNode.type = FileType.Directory;
        fileNode.path = path;
        fileNode.mtime = System.currentTimeMillis();
        return fileNode;
    }

    public static FileNode createFile(String path) {
        FileNode fileNode = new FileNode();
        fileNode.type = FileType.File;
        fileNode.path = path;
        fileNode.mtime = System.currentTimeMillis();
        return fileNode;
    }


    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public long getMtime() {
        return mtime;
    }

    public FileType getType() {
        return type;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public String getName() {
        int index = path.lastIndexOf("/");
        return index == -1 ? path : path.substring(index + 1);
    }

    public FileNode getParent() {return parent;}
    public void setParent(FileNode parent) { this.parent = parent; }

    public List<FileNode> getChildren() {
        return children;
    }

    public boolean hasChild() {
        return children != null && children.size() > 0;
    }
    public FileNode getChild(String name) {
        if (type != FileType.Directory || children == null || children.size() == 0){
            return null;
        }
        Optional<FileNode> any = children.stream()
                .filter(statInfo -> statInfo.path.equals(name)).findAny();
        return any.orElse(null);
    }

    public void addChild(FileNode fileNode) {
        if (children == null) {
            children = new LinkedList<>();
        }
        children.add(fileNode);
    }

    public void removeChild(FileNode fileNode) {
        if (children != null) {
            children.removeIf(info -> info.getPath().equals(fileNode.getPath()));
        }
    }

    public List<ReplicaData> getReplicaDataList() {
        if (replicaDataList == null) {
            replicaDataList = new LinkedList<>();
        }
        return replicaDataList;
    }

    public void setReplicaDataList(List<ReplicaData> replicaDataList) {
        this.replicaDataList = replicaDataList;
    }


    public String getReplicaString() {
        String replicaPorts = "[";
        if (replicaDataList != null) {
            List<String> ports = new LinkedList<>();
            for (ReplicaData data : replicaDataList) {
                ports.add(data.getDsNode());
            }
            replicaPorts += Arrays.toString(ports.toArray(new String[0]));
        }
        replicaPorts += "]";
        return replicaPorts;
    }
    @Override
    public String toString() {

        return "StatInfo{" +
                "path='" + path + '\'' +
                ", size=" + size +
                ", mtime=" + mtime +
                ", type=" + type +
                ", replica=" + getReplicaString() +
                '}';
    }

    public String info() {

        return path + " - " + type + " - " + size + "B" + " - " + mtime + " - " + getReplicaString();
    }
}
