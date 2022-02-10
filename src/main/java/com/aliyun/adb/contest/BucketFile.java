package com.aliyun.adb.contest;

import sun.nio.ch.DirectBuffer;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.aliyun.adb.contest.RaceAnalyticDB.*;

public class BucketFile implements Serializable {
    public ByteBuffer byteBuffer;
    public long writePosition;
    public int bufferIndex;

    public long address;
    public long initAddress;
    public boolean isMonitor;
    public int partitionNo;
    public long tableID;

    public boolean destroy() {
        return isMonitor;
    }

    public BucketFile(boolean isMonitor) {
        this.isMonitor = isMonitor;
    }

    public BucketFile(int partitionNo) {
        writePosition = 0;
        try {
            byteBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
            bufferIndex = 0;
            address = ((DirectBuffer) byteBuffer).address();
            initAddress = address;
            this.partitionNo = partitionNo;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public BucketFile(ByteBuffer byteBuffer) {
        writePosition = 0;
        try {
            this.byteBuffer = byteBuffer;
            bufferIndex = 0;
            address = ((DirectBuffer) byteBuffer).address();
            initAddress = address;
            this.partitionNo = -1;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}