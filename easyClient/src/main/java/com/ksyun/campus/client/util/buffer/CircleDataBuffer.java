package com.ksyun.campus.client.util.buffer;

public class CircleDataBuffer {
    private final byte[] buffer;
    private int size = 0;
    private int readIdx  = 0; // 读取索引
    private int writeIdx = 0; // 写入索引

    public CircleDataBuffer(int capacity) {
        buffer = new byte[capacity];
    }

    /**
     * src -> buffer
     */
    public void copyFrom(byte[] src, int len) {
        System.arraycopy(src, 0, buffer, 0, len);
        this.size = len;
        this.writeIdx = len;
        this.readIdx = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isFull() {
        return size == buffer.length;
    }

    /**
     * 读出数据: buffer -> dst
     * return: 读取 byte 个数
     */
    public int read(byte[] dst) {
        if (isEmpty()) return 0;

        int len = Math.min(size, dst.length); // 最多读出数据
        if (readIdx + len <= buffer.length) {
            // 不需要跨界
            System.arraycopy(buffer, readIdx, dst, 0, len);
            readIdx += len;
        } else {
            // 需要跨界
            int t = buffer.length - readIdx;
            System.arraycopy(buffer, readIdx, dst, 0, t);
            System.arraycopy(buffer, 0, dst, t, len - t);
            readIdx = len - t;
        }
        size -= len;
        return len;
    }

    /**
     * 写入数据: dst -> buffer
     */
    public int write(byte[] src) {
        if (isFull()) return 0;
        int len = Math.min(src.length, buffer.length - size);
        if (writeIdx + len <= buffer.length) {
            // 无需跨界
            System.arraycopy(src, 0, buffer, writeIdx, len);
            writeIdx += len;
        } else {
            // 需要跨界
            int t = buffer.length - writeIdx;
            System.arraycopy(src, 0, buffer, writeIdx, t);
            System.arraycopy(src, t, buffer, 0, len - t);
            writeIdx = len - t;
        }
        size += len;
        return len;
    }

    public int size() {
        return size;
    }

    public int freeSpace() {
        return buffer.length - size;
    }

    public int capacity() {
        return buffer.length;
    }

    public void info() {
        System.out.println("readIdx: " + readIdx + ", writeIdx: " + writeIdx + ", size: " + size);
        for (int i = readIdx; i < size; ++i) {
            System.out.print(buffer[i % buffer.length] + " ");
        }
        System.out.println();
    }

    public byte[] getData() {
        byte[] data = new byte[this.size];
        this.read(data);
        return data;
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

    public static int byteArrayToInt(byte[] array) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (array[i] & 0xFF);
        }
        return value;
    }
}
