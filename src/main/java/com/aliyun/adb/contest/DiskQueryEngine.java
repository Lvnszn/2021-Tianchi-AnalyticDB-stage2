package com.aliyun.adb.contest;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.aliyun.adb.contest.RaceAnalyticDB.*;
import static com.aliyun.adb.contest.UnsafeUtil.unsafe;


public class DiskQueryEngine {
    public FileWriter[] fileWriters;
    private String workDir;
    private String filePrefix;
    private static final ThreadLocal<long[]> sharedBufferThreadLocal = ThreadLocal.withInitial(() -> new long[20_0000]);
    private static final ThreadLocal<long[][][]> sharedBuffers = ThreadLocal.withInitial(() -> new long[2][PARTITION_OVER_PARTITION][10_0000]);
    private static final ThreadLocal<int[][]> shared = ThreadLocal.withInitial(() -> new int[2][PARTITION_OVER_PARTITION]);
    private static final ThreadLocal<ByteBuffer[]> sharedByte = ThreadLocal.withInitial(() -> {
        ByteBuffer[] bufs = new ByteBuffer[2];
        for (int i = 0; i < 2; i++) {
            bufs[i] = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
        }
        return bufs;
    });

    public DiskQueryEngine(String workDir, String filePrefix) {
        this.workDir = workDir;
        this.filePrefix = filePrefix;
        fileWriters = new FileWriter[PARTITION];
    }

    public void init(int threadNo) {
        for (int j = 0; j < PARTITION; j++) {
            fileWriters[j] = new FileWriter(workDir + File.separator + filePrefix + "_" + threadNo + "_" + j);
        }
    }

//    public long quantile(double percentile) throws Exception {
////        int totalNumer = getTotalNumer();
////        System.out.println(totalNumer);
//        int rank = (int)Math.round(RESULT_SIZE * percentile);
//        // rank 代表第 n 个数，从 1 开始；index 代表数组的下标，从 0 开始
//        int currentBucketBeginRank = 1;
//        int currentBucketEndRank;
//        int blockIndex = 0;
//        int hitPartition = -1;
//        int WRITE_THREAD_NUM = 1;
//        for (int i = 0; i < PARTITION; i++) {
//            int dataNum = 0;
//            for (int j = 0; j < WRITE_THREAD_NUM; j++) {
//                dataNum += (fileWriters[i].writePosition.get()/7);
//            }
//            currentBucketEndRank = currentBucketBeginRank + dataNum;
//            if (currentBucketBeginRank <= rank && rank < currentBucketEndRank) {
//                blockIndex = rank - currentBucketBeginRank;
//                hitPartition = i;
//                break;
//            }
//            currentBucketBeginRank = currentBucketEndRank;
//        }
//
//        long[][][] nums = sharedBuffers.get();
//        int[][] sharedIndex = shared.get();
//        ByteBuffer[] buf = sharedByte.get();
//        for (int j = 0; j < PARTITION_OVER_PARTITION; j++) {
//            sharedIndex[0][j] = 0;
//            sharedIndex[1][j] = 0;
//            sharedIndex[2][j] = 0;
//        }
////        long ioStart = System.currentTimeMillis();
//
//        long headByte = ((long) hitPartition << (64 - OFFSET));
//        Future[] futures = new Future[3];
//        for (int i = 0; i < 3; i++) {
////            bucketFiles[i][hitPartition].flush();
//            futures[i] = fileWriters[hitPartition].loadFileAsync(nums[i], sharedIndex[i], i, headByte, buf[i]);
//        }
//        for (Future future : futures) {
//            try {
//                future.get();
//            } catch (Exception e) {
//                e.printStackTrace();
////                 数据不均匀会导致这里溢出
//                return 0;
//            }
//        }
////        System.out.println("io cost " + (System.currentTimeMillis() - ioStart) + " ms");
//
//        int currentBlockBeginIndex = 0;
//        int currentBlockEndIndex;
//        int resultIndex = 0;
//        int hitmBlock = -1;
//        for (int i = 0; i < PARTITION_OVER_PARTITION; i++) {
//            int dataNum = 0;
//            for (int j = 0; j < 3; j++) {
//                dataNum += sharedIndex[j][i];
//            }
//            currentBlockEndIndex = currentBlockBeginIndex + dataNum;
//            if (currentBlockBeginIndex <= blockIndex && blockIndex < currentBlockEndIndex) {
//                resultIndex = blockIndex - currentBlockBeginIndex;
//                hitmBlock = i;
//                break;
//            }
//            currentBlockBeginIndex = currentBlockEndIndex;
//        }
//
//        long[] sharedBuffer = sharedBufferThreadLocal.get();
//        int sharedBufferIndex = 0;
//        for (int i = 0; i < 3; i++) {
//            for (int j = 0; j < sharedIndex[i][hitmBlock]; j++) {
//                sharedBuffer[sharedBufferIndex++] = nums[i][hitmBlock][j];
//            }
//        }
//
////        long start = System.currentTimeMillis();
//        long result = Util.quickSelect(sharedBuffer, 0, sharedBufferIndex - 1, sharedBufferIndex - resultIndex);
////        System.out.println("sort cost " + (System.currentTimeMillis() - start) + " ms");
//        return result;
//    }


    public long quantile(double percentile, int index, long[] postition) throws Exception {
//        int totalNumer = getTotalNumer();
//        System.out.println(totalNumer);
        int rank = (int)Math.round(RESULT_SIZE * percentile);
        // rank 代表第 n 个数，从 1 开始；index 代表数组的下标，从 0 开始
        int currentBucketBeginRank = 1;
        int currentBucketEndRank;
        int blockIndex = 0;
        int hitPartition = -1;
        long startPosition = 0;
        long endPosition = 0;

        for (int i = 0; i < PARTITION; i++) {
            int dataNum = 0;
            endPosition = postition[i + index * PARTITION];
            if (index == 1) {
                startPosition = postition[i];
            }
            dataNum += ((endPosition - startPosition) /7);
            currentBucketEndRank = currentBucketBeginRank + dataNum;
            if (currentBucketBeginRank <= rank && rank < currentBucketEndRank) {
                blockIndex = rank - currentBucketBeginRank;
                hitPartition = i;
                break;
            }
            currentBucketBeginRank = currentBucketEndRank;
        }

        long[][][] nums = sharedBuffers.get();
        int[][] sharedIndex = shared.get();
        ByteBuffer[] buf = sharedByte.get();
        for (int j = 0; j < PARTITION_OVER_PARTITION; j++) {
            sharedIndex[0][j] = 0;
            sharedIndex[1][j] = 0;
        }
//        long ioStart = System.currentTimeMillis();

        long headByte = ((long) hitPartition << (64 - OFFSET));
        Future[] futures = new Future[2];
        for (int i = 0; i < 2; i++) {
//            bucketFiles[i][hitPartition].flush();
            futures[i] = loadFileAsync(startPosition, endPosition, fileWriters[hitPartition].fileChannel, nums[i], sharedIndex[i], i, headByte, buf[i]);
        }
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
//                 数据不均匀会导致这里溢出
                return 0;
            }
        }
//        System.out.println("io cost " + (System.currentTimeMillis() - ioStart) + " ms");

        int currentBlockBeginIndex = 0;
        int currentBlockEndIndex;
        int resultIndex = 0;
        int hitmBlock = -1;
        for (int i = 0; i < PARTITION_OVER_PARTITION; i++) {
            int dataNum = 0;
            for (int j = 0; j < 2; j++) {
                dataNum += sharedIndex[j][i];
            }
            currentBlockEndIndex = currentBlockBeginIndex + dataNum;
            if (currentBlockBeginIndex <= blockIndex && blockIndex < currentBlockEndIndex) {
                resultIndex = blockIndex - currentBlockBeginIndex;
                hitmBlock = i;
                break;
            }
            currentBlockBeginIndex = currentBlockEndIndex;
        }

        long[] sharedBuffer = sharedBufferThreadLocal.get();
        int sharedBufferIndex = 0;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < sharedIndex[i][hitmBlock]; j++) {
                sharedBuffer[sharedBufferIndex++] = nums[i][hitmBlock][j];
            }
        }

//        long start = System.currentTimeMillis();
//        long result = Util.quickSelect(sharedBuffer, 0, sharedBufferIndex - 1, sharedBufferIndex - resultIndex);
        return sharedBuffer[Util.quickSelectV2(sharedBuffer, 0, sharedBufferIndex - 1, resultIndex + 1)];
//        System.out.println("sort cost " + (System.currentTimeMillis() - start) + " ms");
//        return result;
    }

    public Future<Boolean> loadFileAsync(final long startPosition, final long endPosition, FileChannel fileChannel, final long[][] nums, final int[] index, int threadNo, long headValue, ByteBuffer bf) {
        Future<Boolean> future = executorService.submit(() -> {
            bf.clear();
            long bucketSize = (endPosition - startPosition);
            long fileSize = ( bucketSize / 7 / 2 + 1)*7;
            long partitionTotalSize;
            if (threadNo == 1) {
                partitionTotalSize = startPosition + bucketSize;
            } else {
                partitionTotalSize = startPosition + fileSize * (threadNo + 1);
            }
            long readPosition = startPosition;
            if (threadNo != 0) {
                readPosition = startPosition + fileSize*threadNo;
            }

            while (readPosition < partitionTotalSize - 1) {
                int size = (int)Math.min(WRITE_BUFFER_SIZE, partitionTotalSize - readPosition);
                try {
                    fileChannel.read(bf, readPosition);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                readPosition += size;
                long addresses = ((DirectBuffer) bf).address();
                for (int j = 0; j < size / 7; j++) {
                    long longVal = unsafe.getLong(addresses)&0x00FFFFFFFFFFFFFFL|headValue;
                    addresses += 7;
                    int p = (int) ((longVal >> (64 - OFFSET - 3)) & 0x07);
                    nums[p][index[p]++] = longVal;
                }

                bf.clear();
            }

            return true;
        });
        return future;
    }

    private int partition(long a[], int i, int j) {
        long tmp = a[j];
        int index = i;
        if (i < j) {
            for (int k = i; k < j; k++) {
                if (a[k] >= tmp) {
                    swap(a, index++, k);
                }
            }
            swap(a, index, j);
            return index;
        }
        return index;
    }

    private long search(long a[], int i, int j, int k) {
        int m = partition(a, i, j);
        if (k == m - i + 1) { return a[m]; } else if (k < m - i + 1) {
            return search(a, i, m - 1, k);
        }
        //后半段
        else {
            //核心后半段：再找第 k-(m-i+1)大的数就行
            return search(a, m + 1, j, k - (m - i + 1));
        }
    }

    //交换数组array中的下标为index1和index2的两个数组元素
    private void swap(long[] array, int index1, int index2) {
        long temp = array[index1];
        array[index1] = array[index2];
        array[index2] = temp;
    }

//    private int getTotalNumer() {
//        int sum = 0;
//        for (int j = 0; j < PARTITION; j++) {
//            sum += fileWriters[j].getDataNum();
//        }
//        return sum;
//    }
}