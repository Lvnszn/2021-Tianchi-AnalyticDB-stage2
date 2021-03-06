//package com.aliyun.adb.contest;
//
//import java.io.*;
//import java.nio.ByteBuffer;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.channels.FileChannel.MapMode;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import com.aliyun.adb.contest.spi.AnalyticDB;
//import sun.nio.ch.DirectBuffer;
//
//import static com.aliyun.adb.contest.UnsafeUtil.unsafe;
//
//public class TotalAnalyticDB implements AnalyticDB {
//
//    private final Map<String, DiskQueryEngine> columnName2EngineMap = new HashMap<>();
//
//    public static final int THREAD_NUM = 4;
//    public static final int DISPATCH_THREAD_NUM = 1;
//
//    public static final int READ_THREAD_NUM = 4;
//    public static final int WRITE_THREAD_NUM = 3;
//
//    public static final int PARTITION_OVER_PARTITION = 8;
//
//    //    public static final int PARTITION = 128;
//    //    public static final int OFFSET = 8;
//    public static final int PARTITION = 2048;
//    public static final int OFFSET = 12;
//    //    public static final int WRITE_BUFFER_SIZE = 32 * 7;
//    //    public static final int ARRAY_SIZE = WRITE_BUFFER_SIZE / 7;
//    //    public static final int READ_BLOCK_SIZE = 1024;
//    //public static final int RESULT_SIZE = 10000;
//    public static final int RESULT_SIZE = 1000000000;
//    public static final int WRITE_BUFFER_SIZE = 126*1024;
//    public static final int READ_BLOCK_SIZE = 8*1024*1024;
//    //    public static final int MAP_BLOCK_SIZE = 32*1024*1024;
//    public static final int ARRAY_SIZE = (int) (READ_BLOCK_SIZE / 40 * 1.2);
//    public static final int bufferSize = WRITE_BUFFER_SIZE / 7;
//    //-    public static final int FREESIZE = 100;
//    public static final double FlushRatio = 1.5;
//    public static final int FREESIZE = 36;
//
//    public static ExecutorService executorService = Executors.newFixedThreadPool(8);
//    public ConcurrentLinkedQueue<BucketFile> arrayBlockingQueue = new ConcurrentLinkedQueue<>();
//    public ConcurrentLinkedQueue<BucketFile> nextArrayBlockingQueue = new ConcurrentLinkedQueue<>();
//    public ConcurrentLinkedQueue<ByteBuffer> bytesArrayBlockingQueue = new ConcurrentLinkedQueue<>();
//
//    public ConcurrentLinkedQueue<long[]> arrayLongQueue = new ConcurrentLinkedQueue<>();
//    public ConcurrentLinkedQueue<long[]> partArrayLongQueue = new ConcurrentLinkedQueue<>();
//
//    public ConcurrentLinkedQueue<BucketFile> freeBufferQueue = new ConcurrentLinkedQueue<>();
//    public ConcurrentLinkedQueue<ByteBuffer> freeBytesQueue = new ConcurrentLinkedQueue<>();
//    public ConcurrentLinkedQueue<long[]> freeLongQueue = new ConcurrentLinkedQueue<>();
//
//    private final AtomicInteger first = new AtomicInteger(1);
//    public DiskQueryEngine orderQueryEngine;
//    public DiskQueryEngine partQueryEngine;
//    public File partMeta;
//    public File orderMeta;
//    public long[] orderKeyPosition = new long[PARTITION * 2];
//    public long[] partKeyPosition = new long[PARTITION * 2];
//    public int orderIdx = 0;
//    public int partIdx = 0;
//
//    /**
//     * The implementation must contain a public no-argument constructor.
//     */
//    public TotalAnalyticDB() {
//        try {
//            orderMeta = new File("/adb-data/player/order_meta");
//            if (!orderMeta.exists()) {
//                orderMeta.createNewFile();
//            }
//
//            partMeta = new File("/adb-data/player/part_meta");
//            if (!partMeta.exists()) {
//                partMeta.createNewFile();
//            } else {
//                initPosition("/adb-data/player");
//
////                long s = 0;
////                long f = 0;
////                for (int i = 0; i < PARTITION; i++) {
////                    f += partKeyPosition[i];
////                    s += partKeyPosition[PARTITION + i] - partKeyPosition[i];
////                    System.out.println("endPosition:" + partKeyPosition[PARTITION + i] + " startPosition" + partKeyPosition[i] + " totalSize:" + s + " first totalSize:" + f);
////                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
////        new Thread(()-> {
////            try {
////                Thread.sleep(50*1000);
////                System.exit(0);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
////        }).start();
//    }
//
//    //    @Override
//    public void aload(String tpchDataFileDir, String workspaceDir) throws Exception {
//        File dir = new File(tpchDataFileDir);
//        new Thread(VmstatLogger::useLinuxCommond2).start();
//        final int threadNum = READ_THREAD_NUM;
//        for (File dataFile : Objects.requireNonNull(dir.listFiles())) {
//            File meta = new File(workspaceDir + File.separator + dataFile.getName() + "_meta");
//            if (meta.exists()) {
//                meta = new File(workspaceDir + File.separator + dataFile.getName() + "_recovery");
//            }
//            meta.createNewFile();
//
//            if ("lineitem".equals(dataFile.getName()) || "orders".equals(dataFile.getName())) {
//                long start = System.currentTimeMillis();
//                int a = 0;
//                File n = new File(workspaceDir + File.separator + dataFile.getName() + "_recovery");
//                if (n.exists()) {
//                    a = 1;
//                }
//
//                if (a == 1) {
//                    continue;
//                }
//
//                if (first.compareAndSet(1, 2)) {
//
//                    Future[] fs = new Future[DISPATCH_THREAD_NUM * 2 + 2];
//                    fs[0] = executorService.submit(() -> {
//                        String col = "ORDERKEY";
//                        orderQueryEngine = new DiskQueryEngine(workspaceDir, col);
//                        columnName2EngineMap.put(col, orderQueryEngine);
//                        orderQueryEngine.init(0);
//                    });
//
//                    fs[1] = executorService.submit(() -> {
//                        String col = "PARTKEY";
//                        partQueryEngine = new DiskQueryEngine(workspaceDir, col);
//                        columnName2EngineMap.put(col, partQueryEngine);
//                        partQueryEngine.init(0);
//                    });
//
//                    for (int futureNo = 0; futureNo < DISPATCH_THREAD_NUM * 2; futureNo++) {
//                        fs[futureNo + 2] = executorService.submit(() -> {
//                            for (int i = 0; i < PARTITION * DISPATCH_THREAD_NUM * 2; i++) {
//                                freeBufferQueue.add(new BucketFile(ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE)));
//                            }
//                        });
//                    }
//
//                    new Thread(() -> {
//                        for (int i = 0; i < FREESIZE; i++) {
//                            freeBytesQueue.add(ByteBuffer.allocateDirect(READ_BLOCK_SIZE));
//                        }
//                    }).start();
//
//                    new Thread(() -> {
//                        for (int i = 0; i < FREESIZE*2; i++) {
//                            freeLongQueue.add(new long[ARRAY_SIZE]);
//                        }
//                    }).start();
//
//                    for (Future f : fs) {
//                        f.get();
//                    }
//                }
//
//                RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
//                FileChannel fileChannel = randomAccessFile.getChannel();
//                long totalSize = fileChannel.size();
//                long[] readThreadPosition = new long[threadNum];
//                // ??????????????? n ??????
//                readThreadPosition[0] = 21;
//                int preReadSize = 50;
//                for (int i = 1; i < threadNum; i++) {
//                    long paddingPosition = totalSize / threadNum * i;
//                    MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, paddingPosition, preReadSize);
//                    for (int j = 0; j < preReadSize; j++) {
//                        if (mappedByteBuffer.get() == (byte) '\n') {
//                            paddingPosition += j + 1;
//                            break;
//                        }
//                    }
//                    readThreadPosition[i] = paddingPosition;
//                }
//
//                CountDownLatch sourceDownLatch = new CountDownLatch(READ_THREAD_NUM);
//
//                for (int k = 0; k < READ_THREAD_NUM; k++) {
//                    final int threadNo = k;
//                    new Thread(() -> {
//                        long pollCost = 0;
//                        long readFileCost = 0;
//                        long threadStart = System.currentTimeMillis();
//                        try {
//                            // int readBufferSize = READ_BLOCK_SIZE;
//                            ByteBuffer byteBuffer = freeBytesQueue.poll();
//                            while (byteBuffer == null) {
//                                Thread.sleep(100);
//                                byteBuffer = freeBytesQueue.poll();
//                            }
//
//                            long readPosition = readThreadPosition[threadNo];
//                            long partitionTotalSize;
//                            if (threadNo == READ_THREAD_NUM - 1) {
//                                partitionTotalSize = totalSize;
//                            } else {
//                                partitionTotalSize = readThreadPosition[threadNo + 1];
//                            }
//                            long address = ((DirectBuffer) byteBuffer).address();
//                            while (readPosition < partitionTotalSize - 1) {
//                                int size = (int) Math.min(64 * 1024 * 1024, partitionTotalSize - readPosition);
////                                byteBuffer.clear();
//                                long s = System.currentTimeMillis();
////                                fileChannel.read(byteBuffer, readPosition);
//                                MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, readPosition, 64 * 1024 * 1024);
//
//                                pollCost += System.currentTimeMillis() - s;
//
//                                int l = size;
//                                while (l > 0) {
//                                    if (mappedByteBuffer.get(l - 1) == '\n') {
////                                        byteBuffer.limit(l);
//                                        break;
//                                    }
//                                    l--;
//                                }
//
//                                long val = 0;
//                                for (int i = 0; i < l; i++) {
//                                    byte temp = mappedByteBuffer.get(i);
//                                    do {
//                                        val = val * 10 + (temp - '0');
//                                        temp = mappedByteBuffer.get((++i));
//                                    } while (temp != ',');
//
//                                    val = 0;
//                                    // skip ???
//                                    i++;
//                                    temp = mappedByteBuffer.get(i);
//                                    do {
//                                        val = val * 10 + (temp - '0');
//                                        temp = mappedByteBuffer.get(++i);
//                                    } while (temp != '\n');
//                                    val = 0;
//                                }
//                                readPosition += size;
//                                // System.out.println(System.currentTimeMillis() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size());
//                            }
//
//                            freeBytesQueue.add(byteBuffer);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        sourceDownLatch.countDown();
//                        System.out.println("read thread cost " + (System.currentTimeMillis() - threadStart) + " ms " + pollCost + "ms" + readFileCost + " ms");
//                    }).start();
//                }
//
//                sourceDownLatch.await();
//            }
//        }
//        System.exit(0);
//    }
//
//    @Override
//    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
//        new Thread(VmstatLogger::useLinuxCommond2).start();
//        DiskRaceEngine orderRaceEngine = new DiskRaceEngine();
//        DiskRaceEngine partRaceEngine = new DiskRaceEngine();
//        final int threadNum = READ_THREAD_NUM;
//        File meta = new File(workspaceDir + File.separator + "orders_meta");
//        meta.createNewFile();
//        meta = new File(workspaceDir + File.separator + "lineitem_meta");
//        meta.createNewFile();
//
//        // ???????????????
//        long createEngineStart = System.currentTimeMillis();
//
//        System.out.println(
//                "create engine cost " + (System.currentTimeMillis() - createEngineStart) + " ms");
//
//        if (first.compareAndSet(1, 2)) {
//            new Thread(() -> {
//                for (int i = 0; i < FREESIZE; i++) {
//                    freeBytesQueue.add(ByteBuffer.allocateDirect(READ_BLOCK_SIZE));
//                }
//            }).start();
//
//            new Thread(() -> {
//                for (int i = 0; i < FREESIZE * 4; i++) {
//                    freeLongQueue.add(new long[ARRAY_SIZE]);
//                }
//            }).start();
//
//            Future[] fs = new Future[DISPATCH_THREAD_NUM * 2 + 1];
////            fs[0] = executorService.submit(() -> {
////                String col = "ORDERKEY";
////                orderQueryEngine = new DiskQueryEngine(workspaceDir, col);
////                columnName2EngineMap.put(col, orderQueryEngine);
////                orderQueryEngine.init(0);
////            });
//
//            fs[0] = executorService.submit(() -> {
//                String col = "PARTKEY";
//                partQueryEngine = new DiskQueryEngine(workspaceDir, col);
//                columnName2EngineMap.put(col, partQueryEngine);
//                partQueryEngine.init(0);
//            });
//
//            for (int futureNo = 0; futureNo < 2; futureNo++) {
//                fs[futureNo + 1] = executorService.submit(() -> {
//                    for (int i = 0; i < PARTITION * 2; i++) {
//                        freeBufferQueue.add(new BucketFile(ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE)));
//                    }
//                });
//            }
//
//            for (Future f : fs) {
//                f.get();
//            }
//        }
//
//        System.out.println(
//                "init parameters cost " + (System.currentTimeMillis() - createEngineStart) + " ms");
//        System.out.println(freeBufferQueue.size() + " " + freeLongQueue.size() + " " + freeBytesQueue.size() + " ");
//
//        int ii = 2;
//        File dataFile = new File(tpchDataFileDir + File.separator + "orders");
//        long start = System.currentTimeMillis();
//        RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
//        FileChannel fileChannel = randomAccessFile.getChannel();
//        long totalSize = fileChannel.size();
//        long[] readThreadPosition = new long[ii];
//        // ??????????????? n ??????
//        readThreadPosition[0] = 21;
//        int preReadSize = 50;
//        for (int i = 1; i < ii; i++) {
//            long paddingPosition = totalSize / ii * i;
//            MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, paddingPosition, preReadSize);
//            for (int j = 0; j < preReadSize; j++) {
//                if (mappedByteBuffer.get() == (byte) '\n') {
//                    paddingPosition += j + 1;
//                    break;
//                }
//            }
//            readThreadPosition[i] = paddingPosition;
//        }
//
//
//        File dataFile2 = new File(tpchDataFileDir + File.separator + "lineitem");
//
//        RandomAccessFile randomAccessFile2 = new RandomAccessFile(dataFile2, "r");
//        FileChannel fileChannel2 = randomAccessFile2.getChannel();
//        long totalSize2 = fileChannel2.size();
//        long[] readThreadPosition2 = new long[ii];
//        // ??????????????? n ??????
//        readThreadPosition2[0] = 21;
//        for (int i = 1; i < ii; i++) {
//            long paddingPosition = totalSize2 / ii * i;
//            MappedByteBuffer mappedByteBuffer = fileChannel2.map(MapMode.READ_ONLY, paddingPosition, preReadSize);
//            for (int j = 0; j < preReadSize; j++) {
//                if (mappedByteBuffer.get() == (byte) '\n') {
//                    paddingPosition += j + 1;
//                    break;
//                }
//            }
//            readThreadPosition2[i] = paddingPosition;
//        }
//
//        CountDownLatch sourceDownLatch = new CountDownLatch(ii*2);
//        CountDownLatch readDownLatch = new CountDownLatch(2 * WRITE_THREAD_NUM);
//        CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
//        CountDownLatch dispatchLatch = new CountDownLatch(DISPATCH_THREAD_NUM * 2);
//
//        for (int k = 0; k < ii; k++) {
//            final int threadNo = k;
//            new Thread(() -> {
//                long pollCost = 0;
//                long readFileCost = 0;
//                long threadStart = System.currentTimeMillis();
//                try {
//                    // int readBufferSize = READ_BLOCK_SIZE;
//                    ByteBuffer byteBuffer = freeBytesQueue.poll();
//                    while (byteBuffer == null) {
//                        Thread.sleep(100);
//                        byteBuffer = freeBytesQueue.poll();
//                    }
//
//                    long readPosition = readThreadPosition[threadNo];
//                    long partitionTotalSize;
//                    if (threadNo == ii - 1) {
//                        partitionTotalSize = totalSize;
//                    } else {
//                        partitionTotalSize = readThreadPosition[threadNo + 1];
//                    }
//
//                    while (readPosition < partitionTotalSize - 1) {
//                        int size = (int) Math.min(READ_BLOCK_SIZE, partitionTotalSize - readPosition);
//                        byteBuffer.clear();
////                                long s = System.currentTimeMillis();
//                        fileChannel.read(byteBuffer, readPosition);
////                                pollCost += System.currentTimeMillis() - s;
//                        long address = ((DirectBuffer) byteBuffer).address();
//                        while (size > 0) {
//                            if (unsafe.getByte(address + size - 1) == '\n') {
//                                byteBuffer.limit(size);
//                                break;
//                            }
//                            size--;
//                        }
//
//                        readPosition += size;
//                        ByteBuffer tmp = byteBuffer;
//                        bytesArrayBlockingQueue.add(tmp);
//                        byteBuffer = freeBytesQueue.poll();
//                        while (byteBuffer == null) {
//                            Thread.sleep(100);
//                            byteBuffer = freeBytesQueue.poll();
//                        }
//                        // System.out.println(System.currentTimeMillis() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size());
//                    }
//                    freeBytesQueue.add(byteBuffer);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                //                        System.out.println(Thread.currentThread().getName() + " finish read");
//                sourceDownLatch.countDown();
//                System.out.println("read thread cost " + (System.currentTimeMillis() - threadStart) + " ms " + pollCost + "ms" + readFileCost + " ms");
//            }).start();
//        }
//
//        for (int k = 0; k < ii; k++) {
//            final int threadNo = k;
//            new Thread(() -> {
//                long pollCost = 0;
//                long readFileCost = 0;
//                long threadStart = System.currentTimeMillis();
//                try {
//                    // int readBufferSize = READ_BLOCK_SIZE;
//                    ByteBuffer byteBuffer = freeBytesQueue.poll();
//                    while (byteBuffer == null) {
//                        Thread.sleep(100);
//                        byteBuffer = freeBytesQueue.poll();
//                    }
//
//                    long readPosition = readThreadPosition2[threadNo];
//                    long partitionTotalSize;
//                    if (threadNo == ii - 1) {
//                        partitionTotalSize = totalSize2;
//                    } else {
//                        partitionTotalSize = readThreadPosition2[threadNo + 1];
//                    }
//
//                    while (readPosition < partitionTotalSize - 1) {
//                        int size = (int) Math.min(READ_BLOCK_SIZE, partitionTotalSize - readPosition);
//                        byteBuffer.clear();
////                                long s = System.currentTimeMillis();
//                        fileChannel2.read(byteBuffer, readPosition);
////                                pollCost += System.currentTimeMillis() - s;
//                        long address = ((DirectBuffer) byteBuffer).address();
//                        while (size > 0) {
//                            if (unsafe.getByte(address + size - 1) == '\n') {
//                                byteBuffer.limit(size);
//                                break;
//                            }
//                            size--;
//                        }
//
//                        readPosition += size;
//                        ByteBuffer tmp = byteBuffer;
//                        bytesArrayBlockingQueue.add(tmp);
//                        byteBuffer = freeBytesQueue.poll();
//                        while (byteBuffer == null) {
//                            Thread.sleep(100);
//                            byteBuffer = freeBytesQueue.poll();
//                        }
//                        // System.out.println(System.currentTimeMillis() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size());
//                    }
//                    freeBytesQueue.add(byteBuffer);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                //                        System.out.println(Thread.currentThread().getName() + " finish read");
//                sourceDownLatch.countDown();
//                System.out.println("read thread cost " + (System.currentTimeMillis() - threadStart) + " ms " + pollCost + "ms" + readFileCost + " ms");
//            }).start();
//        }
//
//        for (int k = 0; k < THREAD_NUM; k++) {
//            // final int threadNo = k;
//            new Thread(() -> {
//                long threadStart = System.currentTimeMillis();
//                long pollCount1 = 0;
//                long pollCount2 = 0;
//                //                        long pollCost = 0;
//                long pollCost1 = 0;
//                long pollCost2 = 0;
//                //                        long pollCost3 = 0;
//                //                        long pollCost4 = 0;
//                //                        long pollCost5 = 0;
//                //                        long pollCost6 = 0;
//                //                        long pollCost7 = 0;
//                //                        long pollCount = 0;
//                //                        long readPollCost = 0;
//                //                        long parseCost = 0;
//                //                        long dispatchCost = 0;
//                //                        long dispatchCost2 = 0;
//
////                        long[] orderLong = new long[ARRAY_SIZE];
////                        long[] partLong = new long[ARRAY_SIZE];
//                int No;
//                int size;
//                long val;
//                long address;
//                ByteBuffer readBufferArray;
//                //                        long pollCount = 0;
//                try {
//                    while (true) {
//                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
//                        readBufferArray = bytesArrayBlockingQueue.poll();
//                        // long readCostStart = System.currentTimeMillis();
//                        while (readBufferArray == null) {
//                            //pollCount++;
//                            Thread.sleep(100);
//                            readBufferArray = bytesArrayBlockingQueue.poll();
//                        }
//                        // readPollCost += System.currentTimeMillis() - readCostStart;
//
//                        size = readBufferArray.limit();
//                        if (size == 0) {
//                            break;
//                        }
//
//                        long[] orderLong = freeLongQueue.poll();
//                        // long o1Start = System.currentTimeMillis();
//                        while (orderLong == null) {
//                            Thread.sleep(100);
//                            orderLong = freeLongQueue.poll();
//                        }
//                        // pollCost1 += System.currentTimeMillis() - o1Start;
//
//                        long[] partLong = freeLongQueue.poll();
//                        // o1Start = System.currentTimeMillis();
//                        while (partLong == null) {
//                            Thread.sleep(100);
//                            partLong = freeLongQueue.poll();
//                        }
//                        // pollCost2 += System.currentTimeMillis() - o1Start;
//
//
//                        No = 0;
//                        val = 0;
//                        address = ((DirectBuffer) readBufferArray).address();
//                        for (int i = 0; i < size; i++) {
//                            byte temp = unsafe.getByte(address + i);
//                            if (unsafe.getByte(address + i + 18) == ',') {
//                                for (int innerIdx = 1; innerIdx < 19; innerIdx++) {
//                                    val = val * 10 + (temp - '0');
//                                    temp = unsafe.getByte(address + i + (innerIdx));
//                                }
//                                i += 18;
//                            } else {
//                                do {
//                                    val = val * 10 + (temp - '0');
//                                    temp = unsafe.getByte(address + (++i));
//                                } while (temp != ',');
//                            }
//                            unsafe.putLong(orderLong, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + No * 8L, val);
////                                    orderLong[No] = val;
//                            val = 0;
//                            // skip ???
//                            i++;
//                            temp = unsafe.getByte(address + i);
//                            if (unsafe.getByte(address + i + 18) == '\n') {
//                                for (int innerIdx = 1; innerIdx < 19; innerIdx++) {
//                                    val = val * 10 + (temp - '0');
//                                    temp = unsafe.getByte(address + i + (innerIdx));
//                                }
//                                i += 18;
//                            } else {
//                                do {
//                                    val = val * 10 + (temp - '0');
//                                    temp = unsafe.getByte(address + (++i));
//                                } while (temp != '\n');
//                            }
//                            unsafe.putLong(partLong, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (No++) * 8L, val);
////                                    partLong[No++] = val;
//                            val = 0;
//                            // skip \n
//                        }
//
//                        freeBytesQueue.add(readBufferArray);
//
//                        orderLong[orderLong.length - 1] = No;
//                        partLong[orderLong.length - 1] = No;
//
//                        long[] tmp = orderLong;
//                        arrayLongQueue.add(tmp);
//                        long[] tmpPart = partLong;
//                        partArrayLongQueue.add(tmpPart);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                countDownLatch.countDown();
//                //                        System.out.println("parse thread cost " + (System.currentTimeMillis() - threadStart) + " ms" + pollCount + " times " + pollCost + " ms" + pollCost1 + " ms" + pollCost2 + " ms" + pollCost3 + " ms" + pollCost4 + " ms" + pollCost5 + " ms" + pollCost6 + " ms" + pollCost7 + " ms");
//                //                        System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
//                //                        System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
//                System.out.println("parse thread cost " + (System.currentTimeMillis() - threadStart) + " " + pollCount1 + " " + pollCost1 + " " + pollCount2 + " " + pollCost2);
//            }).start();
//        }
//
//        for (int k = 0; k < DISPATCH_THREAD_NUM; k++) {
////                    final int threadNo = k;
//            new Thread(() -> {
//                Thread.currentThread().setPriority(9);
//                long threadStart = System.currentTimeMillis();
//                long pollCount = 0;
//                long pollCost = 0;
//                for (int i = 0; i < PARTITION; i++) {
//                    if (orderRaceEngine.bucketFiles[i] == null) {
//                        orderRaceEngine.bucketFiles[i] = freeBufferQueue.poll();
//                    }
//                }
//                long[] toBeParsedArray;
//                int size;
//                long value;
//                int partition;
//                try {
//                    while (true) {
//                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
//                        // long ss = System.currentTimeMillis();
//                        toBeParsedArray = arrayLongQueue.poll();
//                        while (toBeParsedArray == null) {
//                            // pollCount++;
//                            Thread.sleep(100);
//                            toBeParsedArray = arrayLongQueue.poll();
//                        }
//                        // pollCost += System.currentTimeMillis() - ss;
//
//                        size = toBeParsedArray.length;
//                        if (size == 0) {
//                            break;
//                        }
//
//                        // long dispatchTime = System.currentTimeMillis();
//                        BucketFile b;
//                        int idx = 0;
//                        long bound = toBeParsedArray[size - 1];
//
//                        do {
////                                    value = toBeParsedArray[idx++];
//                            value = unsafe.getLong(toBeParsedArray, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (idx++) * 8L);
//                            partition = (int) (value >> (64 - OFFSET));
//                            b = orderRaceEngine.bucketFiles[partition];
//                            unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
//                        } while (idx < bound);
////                                for (int idx = 0; idx < toBeParsedArray[size - 1]; idx++) {
////                                    value = toBeParsedArray[idx];
////                                    partition = (int) (value >> (64 - OFFSET));
////                                    b = orderRaceEngine.bucketFiles[partition];
////                                    unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
////                                }
//
//                        for (int p = 0; p < PARTITION; p++) {
//                            b = orderRaceEngine.bucketFiles[p];
//                            if (bufferSize - b.bufferIndex < FlushRatio * ARRAY_SIZE / PARTITION) {
//                                BucketFile tmp = b;
//                                tmp.partitionNo = p;
//                                arrayBlockingQueue.add(tmp);
//                                b = freeBufferQueue.poll();
//                                while (b == null) {
//                                    Thread.sleep(100);
//                                    b = freeBufferQueue.poll();
//                                }
//                                orderRaceEngine.bucketFiles[p] = b;
//                            }
//                        }
//
////                                for (int idx = 0; idx < size; idx++) {
////                                    do {
////                                        value = toBeParsedArray[idx++];
////                                        partition = (int) (value >> (64 - OFFSET));
////                                        b = orderRaceEngine.bucketFiles[partition];
////                                        unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
////                                    } while (b.bufferIndex < bufferSize && toBeParsedArray[idx] > 0);
////
////                                    if (b.bufferIndex == bufferSize) {
////                                        BucketFile tmp = orderRaceEngine.bucketFiles[partition];
////                                        tmp.partitionNo = partition;
////                                        arrayBlockingQueue.add(tmp);
////                                        b = freeBufferQueue.poll();
////                                        while (b == null) {
////                                            Thread.sleep(100);
////                                            b = freeBufferQueue.poll();
////                                        }
////                                        orderRaceEngine.bucketFiles[partition] = b;
////                                    }
////
////                                    if (toBeParsedArray[idx] == 0) {
////                                        break;
////                                    }
////                                    idx--;
////                                }
//                        freeLongQueue.add(toBeParsedArray);
//                        // dispatchCost += System.currentTimeMillis() - dispatchTime;
//                    }
//
//                    int flushNo = 0;
//                    do {
//                        BucketFile b = orderRaceEngine.bucketFiles[flushNo];
//                        b.partitionNo = flushNo;
//                        arrayBlockingQueue.add(b);
//                        orderRaceEngine.bucketFiles[flushNo++] = null;
//                    } while (flushNo < PARTITION);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                dispatchLatch.countDown();
//                System.out.println("dispatch thread one cost " + (System.currentTimeMillis() - threadStart) + " ms " + System.currentTimeMillis() + " " + pollCount + " " + pollCost);
//                // System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
//                // System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
//            }).start();
//        }
//
//        for (int k = 0; k < DISPATCH_THREAD_NUM; k++) {
////                    final int threadNo = k;
//            new Thread(() -> {
//                long threadStart = System.currentTimeMillis();
//                long pollCount = 0;
//                long pollCost = 0;
//                Thread.currentThread().setPriority(9);
//                for (int i = 0; i < PARTITION; i++) {
//                    if (partRaceEngine.bucketFiles[i] == null) {
//                        partRaceEngine.bucketFiles[i] = freeBufferQueue.poll();
//                    }
//                }
//                long[] toBeParsedArray;
//                int size;
//                long value;
//                int partition;
//                try {
//                    while (true) {
//                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
//                        toBeParsedArray = partArrayLongQueue.poll();
//                        // long ss = System.currentTimeMillis();
//                        while (toBeParsedArray == null) {
//                            Thread.sleep(100);
//                            // pollCount++;
//                            toBeParsedArray = partArrayLongQueue.poll();
//                        }
//                        // pollCost+= System.currentTimeMillis() - ss;
//
//                        size = toBeParsedArray.length;
//                        if (size == 0) {
//                            break;
//                        }
//
//                        // long dispatchTime = System.currentTimeMillis();
//
//                        BucketFile b;
//                        int idx = 0;
//                        long bound = toBeParsedArray[size - 1];
//                        do {
////                          value = toBeParsedArray[idx++];
//                            value = unsafe.getLong(toBeParsedArray, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (idx++) * 8L);
//                            partition = (int) (value >> (64 - OFFSET));
//                            b = partRaceEngine.bucketFiles[partition];
//                            unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
//                        } while (idx < bound);
//
//                        for (int p = 0; p < PARTITION; p++) {
//                            b = partRaceEngine.bucketFiles[p];
//                            if (bufferSize - b.bufferIndex < FlushRatio * ARRAY_SIZE / PARTITION) {
//                                BucketFile tmp = b;
//                                tmp.partitionNo = p;
//                                nextArrayBlockingQueue.add(tmp);
//                                b = freeBufferQueue.poll();
//                                while (b == null) {
//                                    Thread.sleep(100);
//                                    b = freeBufferQueue.poll();
//                                }
//                                partRaceEngine.bucketFiles[p] = b;
//                            }
//                        }
//
////                                BucketFile b;
////                                for (int idx = 0; idx < size; idx++) {
////                                    do {
////                                        value = toBeParsedArray[idx++];
////                                        partition = (int) (value >> (64 - OFFSET));
////                                        b = partRaceEngine.bucketFiles[partition];
////                                        unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
////                                    } while (b.bufferIndex < bufferSize && toBeParsedArray[idx] > 0);
////
////                                    if (b.bufferIndex == bufferSize) {
////                                        BucketFile tmp = partRaceEngine.bucketFiles[partition];
////                                        tmp.partitionNo = partition;
////                                        nextArrayBlockingQueue.add(tmp);
////                                        b = freeBufferQueue.poll();
////                                        while (b == null) {
////                                            Thread.sleep(100);
////                                            b = freeBufferQueue.poll();
////                                        }
////                                        partRaceEngine.bucketFiles[partition] = b;
////                                    }
////
////                                    if (toBeParsedArray[idx] == 0) {
////                                        break;
////                                    }
////                                    idx--;
////                                }
//
//                        // dispatchCost += System.currentTimeMillis() - dispatchTime;
//                        freeLongQueue.add(toBeParsedArray);
//                    }
//
//                    int flushNo = 0;
//                    do {
//                        BucketFile b = partRaceEngine.bucketFiles[flushNo];
//                        b.partitionNo = flushNo;
//                        nextArrayBlockingQueue.add(b);
//                        partRaceEngine.bucketFiles[flushNo++] = null;
//                    } while (flushNo < PARTITION);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                dispatchLatch.countDown();
//                // System.out.println("dispatch thread two cost " + (System.currentTimeMillis() - threadStart) + " ms" + System.currentTimeMillis());
//                System.out.println("dispatch thread two cost " + (System.currentTimeMillis() - threadStart) + " ms " + System.currentTimeMillis() + " " + pollCount + " " + " " + pollCost);
//                // System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
//                // System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
//            }).start();
//        }
//
//        for (int k = 0; k < WRITE_THREAD_NUM; k++) {
//            new Thread(() -> {
//                long writeCost = 0;
//                long pollCost = 0;
////                        long pollCount = 0;
//                long threadStart = System.currentTimeMillis();
//                try {
//                    while (true) {
//                        // long costStart = System.currentTimeMillis();
//                        BucketFile orderBucket = arrayBlockingQueue.poll();
//                        while (orderBucket == null) {
//                            // pollCount++;
//                            Thread.sleep(100);
//                            orderBucket = arrayBlockingQueue.poll();
//                        }
//                        // pollCost += System.currentTimeMillis() - costStart;
//
//                        if (orderBucket.destroy()) {
//                            break;
//                        }
//
//                        // long writeStart = System.currentTimeMillis();
//                        FileWriter fw = partQueryEngine.fileWriters[orderBucket.partitionNo];
//                        orderBucket.byteBuffer.flip();
//                        orderBucket.byteBuffer.limit(orderBucket.bufferIndex * 7);
//                        fw.fileChannel.write(orderBucket.byteBuffer);
////                                fw.writePosition.set(fw.writePosition.addAndGet(orderBucket.bufferIndex*7L));
//                        orderBucket.bufferIndex = 0;
//                        freeBufferQueue.add(orderBucket);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                readDownLatch.countDown();
//                System.out.println("first write thread cost " + (System.currentTimeMillis() - threadStart) + " ms writeCost" + writeCost + " ms" + pollCost + " ms " + System.currentTimeMillis());
////                        System.out.println("parse thread poll " + pollCount + " times");
//            }).start();
//
//            new Thread(() -> {
//                long threadStart = System.currentTimeMillis();
//                long writeCost = 0;
//                long pollCost = 0;
////                        long pollCount = 0;
//                try {
//                    while (true) {
//                        // long costStart = System.currentTimeMillis();
//                        BucketFile partBucket = nextArrayBlockingQueue.poll();
//                        while (partBucket == null) {
//                            // pollCount++;
//                            Thread.sleep(100);
//                            partBucket = nextArrayBlockingQueue.poll();
//                        }
//                        // pollCost += System.currentTimeMillis() - costStart;
//
//                        if (partBucket.destroy()) {
//                            break;
//                        }
//
//                        // long writeStart = System.currentTimeMillis();
//                        FileWriter fw = partQueryEngine.fileWriters[partBucket.partitionNo];
//                        partBucket.byteBuffer.flip();
//                        partBucket.byteBuffer.limit(partBucket.bufferIndex * 7);
//                        fw.fileChannel.write(partBucket.byteBuffer);
////                                fw.writePosition.set(fw.writePosition.addAndGet(partBucket.bufferIndex*7L));
//                        partBucket.bufferIndex = 0;
//                        freeBufferQueue.add(partBucket);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                readDownLatch.countDown();
////                        System.out.println("parse thread poll " + pollCount + " times");
//                System.out.println("second write thread cost " + (System.currentTimeMillis() - threadStart) + " ms, writeCost:" + writeCost + " ms " + pollCost + " ms " + System.currentTimeMillis());
//            }).start();
//        }
//        sourceDownLatch.await();
//
//        for (int i = 0; i < THREAD_NUM; i++) {
//            bytesArrayBlockingQueue.add(ByteBuffer.allocateDirect(0));
//        }
//        countDownLatch.await();
//
//        for (int i = 0; i < DISPATCH_THREAD_NUM; i++) {
//            arrayLongQueue.add(new long[0]);
//            partArrayLongQueue.add(new long[0]);
//        }
//
//        dispatchLatch.await();
//
//        // todo ????????????
//        for (int i = 0; i < WRITE_THREAD_NUM; i++) {
//            arrayBlockingQueue.add(new BucketFile(true));
//            nextArrayBlockingQueue.add(new BucketFile(true));
//        }
//
//        readDownLatch.await();
//
////                long metaStart = System.currentTimeMillis();
//        Future[] fs = new Future[1];
////        fs[0] = executorService.submit(() -> {
////            try {
////                RandomAccessFile fw = new RandomAccessFile(orderMeta, "rw");
////                ByteBuffer b = ByteBuffer.allocateDirect(PARTITION * 8);
//////                        long adds = ((DirectBuffer) b).address();
////                for (int i = 0; i < PARTITION; i++) {
////                    long s = partQueryEngine.fileWriters[i].fileChannel.size();
////                    orderKeyPosition[orderIdx++] = s;
////                    b.putLong(s);
//////                            unsafe.putLong(adds + i * 8L, s);
////                }
////                b.flip();
////                fw.getChannel().write(b, (orderIdx - PARTITION) * 8L);
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
////        });
//
//        fs[0] = executorService.submit(() -> {
//            try {
//                RandomAccessFile fw = new RandomAccessFile(partMeta, "rw");
//                ByteBuffer b = ByteBuffer.allocateDirect(PARTITION * 8);
////                        long adds = ((DirectBuffer) b).address();
//                for (int i = 0; i < PARTITION; i++) {
//                    long s = partQueryEngine.fileWriters[i].fileChannel.size();
//                    partKeyPosition[partIdx++] = s;
//                    b.putLong(s);
////                            unsafe.putLong(adds + i * 8L, s);
//                }
//                b.flip();
//                fw.getChannel().write(b, (partIdx - PARTITION) * 8L);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//
//        for (Future f : fs) {
//            f.get();
//        }
////                System.out.println("write to meta cost" + (System.currentTimeMillis() - metaStart));
//
//        //                for (int j = 0; j < 512; j++) {
//        //                    long dataNum = 0;
//        //                    for (int i = 0; i < threadNum; i++) {
//        //                        dataNum += orderRaceEngine.bucketFiles[i][j].getDataNum();
//        //                    }
//        //                    System.out.println("partition " + j + " has " + dataNum + " nums");
//        //                }
//
////                long s = 0;
////                long f = 0;
////                for (int i = 0; i < PARTITION; i++) {
////                    f += partKeyPosition[i];
////                    s += partKeyPosition[PARTITION + i] - partKeyPosition[i];
////                    System.out.println("endPosition:" + partKeyPosition[PARTITION + i] + " startPosition" + partKeyPosition[i] + " totalSize:" + s + " first totalSize:" + f);
////                }
//
//        System.out.println("read + analysis cost " + (System.currentTimeMillis() - start) + " ms");
//
//        //                for (Map.Entry kv : columnName2EngineMap.entrySet()) {
//        //                    DiskQueryEngine eee = (DiskQueryEngine) kv.getValue();
//        //                    long nums = 0;
//        //                    for (int j = 0; j < PARTITION; j++) {
//        ////                        System.out.println(kv.getKey() + " | thread 0  partition " + j + " nums " +
//        ////                                eee.fileWriters[j].getDataNum());
//        //                        nums += eee.fileWriters[j].getDataNum();
//        //                    }
//        //                    System.out.println(kv.getKey() + " | " + nums);
//        //                }
//
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_0");
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_1");
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_2");
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_0");
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_1");
//        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_2");
//    }
//
//    //    private ConcurrentHashMap<Long, Boolean> a = new ConcurrentHashMap<Long, Boolean>();
//    private AtomicInteger acc = new AtomicInteger(1);
//    //    private AtomicInteger costTime = new AtomicInteger(1);
//
//    public void testUnsafeResult(String path) throws IOException {
//        File testWorkspaceDir = new File(path);
//        RandomAccessFile rfile = new RandomAccessFile(testWorkspaceDir, "r");
//        System.out.println(rfile.length());
//        FileChannel rfc = rfile.getChannel();
//        ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
//        rfc.read(buf2);
//        long adad = ((DirectBuffer) buf2).address();
//        System.out.println(unsafe.getLong(adad));
//        System.out.println(unsafe.getLong(adad+8));
//        System.out.println(unsafe.getLong(adad+8+8));
//        System.out.println(unsafe.getLong(adad+8+8+8));
//        System.out.println(unsafe.getLong(adad+8+8+8+8));
//    }
//
//    @Override
//    public String quantile(String table, String column, double percentile) throws Exception {
//        //        if (acc.getAndIncrement() <= 2) {
//        //            new Thread(VmstatLogger::useLinuxCommond2).start();
//        //        }
//        int t = acc.getAndIncrement();
//        if (t > 3990) {
//            System.out.println(System.currentTimeMillis());
//        }
//        if (acc.compareAndSet(100, 0)) {
//            throw new RuntimeException("test");
//        }
//        //        if (!a.containsKey(Thread.currentThread().getId())) {
//        //            a.put(Thread.currentThread().getId(), true);
//        //            System.out.println(a.size());
//        //            if (a.size() > 8) {
//        //                throw new RuntimeException("gt 8");
//        //            } else if (acc.compareAndSet(200, 0)) {
//        //                throw new RuntimeException("test");
//        //            }
//        //        }
//
//        //        System.out.println("=============");
//        //        for (Map.Entry kv : columnName2EngineMap.entrySet()) {
//        //            DiskRaceEngine partRaceEngine = (DiskRaceEngine) kv.getValue();
//        //            int a = 0;
//        //            for (int i = 0; i < WRITE_THREAD_NUM; i++) {
//        //                for (int j = 0; j < PARTITION; j++) {
//        ////                    System.out.println(kv.getKey() + " | thread " + i + " partition " + j + " nums " +
//        ////                            partRaceEngine.bucketFiles[i][j].getDataNum());
//        //                    a += partRaceEngine.bucketFiles[i][j].getDataNum();
//        //                }
//        //            }
//        //            System.out.println(a);
//        //        }
//
//        long start = System.currentTimeMillis();
//        DiskQueryEngine raceEngine = columnName2EngineMap.get(tableColumnKey(column));
//        long[] position = tableColumnPosition(column);
//        long ans;
//        if (table.equals("lineitem")) {
//            ans = raceEngine.quantile(percentile, 0, position);
//        } else {
//            ans = raceEngine.quantile(percentile, 1, position);
//        }
//        long cost = System.currentTimeMillis() - start;
//        //        costTime.addAndGet((int) cost);
//        System.out.println(
//                "Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans + ", Cost " + cost + " ms");
//        return ans + "";
//    }
//
//    //    @Override
//    //    public String quantile(String table, String column, double percentile) throws Exception {
//    //        long start = System.currentTimeMillis();
//    //        System.out.println(table + " " + column);
//    //        DiskRaceEngine diskRaceEngine = columnName2EngineMap.get(tableColumnKey(table, column));
//    //        System.out.println(diskRaceEngine == null);
//    //        long ans = diskRaceEngine.quantile(percentile);
//    //        long cost = System.currentTimeMillis() - start;
//    //        System.out.println(
//    //            "Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans + ", Cost " + cost + " ms");
//    //        //if (atomicInteger.incrementAndGet() == 9) {
//    //        //    return "12345";
//    //        //}
//    //        return ans + "";
//    //    }
//
//    private String tableColumnKey(String column) {
//        if (column.equals("L_ORDERKEY") || column.equals("O_ORDERKEY")) {
//            return "ORDERKEY";
//        }
//
//        return "PARTKEY";
////        return (table + "." + column).toLowerCase();
//    }
//
//    private long[] tableColumnPosition(String column) {
//        if (column.equals("L_ORDERKEY") || column.equals("O_ORDERKEY")) {
//            return orderKeyPosition;
//        }
//
//        return partKeyPosition;
//    }
//
//    public void initPosition(String workspaceDir) throws Exception {
//        Future[] fs = new Future[4];
//        fs[0] = executorService.submit(() -> {
//            try {
//                RandomAccessFile r = new RandomAccessFile(orderMeta, "rw");
//                FileChannel fc = r.getChannel();
//                ByteBuffer bf = ByteBuffer.allocate(PARTITION * 2 * 8);
//                bf.clear();
//                fc.read(bf);
//                bf.flip();
//                int idx = 0;
//                while (bf.hasRemaining()) {
//                    orderKeyPosition[idx++] = bf.getLong();
//                }
//                bf.clear();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//
//        fs[1] = executorService.submit(() -> {
//            try {
//                RandomAccessFile r = new RandomAccessFile(partMeta, "rw");
//                FileChannel fc = r.getChannel();
//                ByteBuffer bf = ByteBuffer.allocate(PARTITION * 2 * 8);
//                bf.clear();
//                fc.read(bf);
//                bf.flip();
//                int idx = 0;
//                while (bf.hasRemaining()) {
//                    partKeyPosition[idx++] = bf.getLong();
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//
//        fs[2] = executorService.submit(() -> {
//            String col = "ORDERKEY";
//            orderQueryEngine = new DiskQueryEngine(workspaceDir, col);
//            columnName2EngineMap.put(col, orderQueryEngine);
//            orderQueryEngine.init(0);
//        });
//
//        fs[3] = executorService.submit(() -> {
//            String col = "PARTKEY";
//            partQueryEngine = new DiskQueryEngine(workspaceDir, col);
//            columnName2EngineMap.put(col, partQueryEngine);
//            partQueryEngine.init(0);
//        });
//
//        for (Future f : fs) {
//            f.get();
//        }
//    }
//}