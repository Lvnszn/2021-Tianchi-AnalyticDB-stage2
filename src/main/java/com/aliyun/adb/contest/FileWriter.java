package com.aliyun.adb.contest;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliyun.adb.contest.RaceAnalyticDB.*;
import static com.aliyun.adb.contest.RaceAnalyticDB.WRITE_BUFFER_SIZE;
import static com.aliyun.adb.contest.UnsafeUtil.unsafe;

public class FileWriter implements Serializable {
    public FileChannel fileChannel;
    public AtomicLong writePosition = new AtomicLong(0);

    public FileWriter(String fileName) {
        File file = new File(fileName);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
//            if (randomAccessFile.length() > 0) {
//                writePosition.set(randomAccessFile.length());
//            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

//    public int getDataNum() {
//        return (int) (writePosition.get() / 7);
//    }

//    public Future<Boolean> loadAsync(final long[][] nums, final int[] index, int hitPartition, int threadNo) {
//        Future<Boolean> future = executorService.submit(() -> {
//            ByteBuffer bf = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
//            bf.clear();
//            int readNo = (int) (writePosition.get() / (WRITE_BUFFER_SIZE)) + (writePosition.get() % WRITE_BUFFER_SIZE == 0 ? 0 : 1);
//            long readPosition = 0;
//            for (int i = 0; i < readNo; i++) {
//                int readSize = 0;
//                try {
//                    readSize = fileChannel.read(bf, readPosition);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                readPosition += WRITE_BUFFER_SIZE;
//                long addresses = ((DirectBuffer) bf).address();
//                for (int j = 0; j < readSize / 8; j++) {
//                    long longVal = unsafe.getLong(addresses);
//                    addresses += 8;
////                    long longVal = unsafe.getLong(longBytes, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET);
//                    int p = (int) ((longVal >> (64 - OFFSET - 3)) & 0x07);
//                    nums[p][index[p]++] = longVal;
//                }
//                bf.clear();
//            }
//            return true;
//        });
//        return future;
//    }


//    public Future<Boolean> loadFileAsync(final long[][] nums, final int[] index, int threadNo, long headValue, ByteBuffer bf) {
//        Future<Boolean> future = executorService.submit(() -> {
//            bf.clear();
//            long fileSize = (writePosition.get() / 7 / 3 + 1)*7;
//            long partitionTotalSize;
//            if (threadNo == 2) {
//                partitionTotalSize = fileChannel.size();
//            } else {
//                partitionTotalSize = fileSize * (threadNo + 1);
//            }
//            long readPosition = 0;
//            if (threadNo != 0) {
//                readPosition = fileSize*threadNo;
//            }
//
//            while (readPosition < partitionTotalSize - 1) {
//                int size = (int)Math.min(WRITE_BUFFER_SIZE, partitionTotalSize - readPosition);
//                try {
//                    fileChannel.read(bf, readPosition);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                readPosition += size;
//                long addresses = ((DirectBuffer) bf).address();
//                for (int j = 0; j < size / 7; j++) {
//                    long longVal = unsafe.getLong(addresses)&0x00FFFFFFFFFFFFFFL|headValue;
//                    addresses += 7;
//                    int p = (int) ((longVal >> (64 - OFFSET - 3)) & 0x07);
//                    nums[p][index[p]++] = longVal;
//                }
//
//                bf.clear();
//            }
//
//            return true;
//        });
//        return future;
//    }

//    public void load(long[] nums, int offset) throws Exception {
//        ByteBuffer byteBuffer =  ByteBuffer.allocate(WRITE_BUFFER_SIZE);
//        int readNo = (int)(writePosition / WRITE_BUFFER_SIZE) + (writePosition % WRITE_BUFFER_SIZE == 0 ? 0 : 1);
//        long readPosition = 0;
//        int n = offset;
//        for (int i = 0; i < readNo; i++) {
//            int readSize = fileChannel.read(byteBuffer, readPosition);
//            readPosition += WRITE_BUFFER_SIZE;
//            byteBuffer.flip();
//            for (int j = 0; j < readSize / 8; j++) {
//                byteBuffer.position(j * 8);
//                nums[n] = byteBuffer.getLong();
//                n++;
//            }
//            byteBuffer.clear();
//        }
//    }
}