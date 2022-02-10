package com.aliyun.adb.contest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.adb.contest.spi.AnalyticDB;
import sun.nio.ch.DirectBuffer;

import static com.aliyun.adb.contest.UnsafeUtil.unsafe;

public class RaceAnalyticDB implements AnalyticDB {

    private final Map<String, DiskQueryEngine> columnName2EngineMap = new HashMap<>();

    public static final int THREAD_NUM = 4;
    public static final int DISPATCH_THREAD_NUM = 1;

    public static final int READ_THREAD_NUM = 4;
    public static final int WRITE_THREAD_NUM = 3;

    public static final int PARTITION_OVER_PARTITION = 8;

    //    public static final int PARTITION = 128;
    //    public static final int OFFSET = 8;
    public static final int RRE_SIZE = 40;
    public static final int PARTITION = 2048;
    public static final int OFFSET = 12;
    public static final int MOVED = 64 - OFFSET;
    //    public static final int WRITE_BUFFER_SIZE = 32 * 7;
    //    public static final int ARRAY_SIZE = WRITE_BUFFER_SIZE / 7;
    //    public static final int READ_BLOCK_SIZE = 1024;
    //public static final int RESULT_SIZE = 10000;
    public static final int RESULT_SIZE = 1000000000;
    public static final int WRITE_BUFFER_SIZE = 126*1024;
    public static final int FLUSH_BUFFER_SIZE = 4*56*1024;
    public static final int READ_BLOCK_SIZE = 8*1024*1024 + RRE_SIZE;
    public static final int READ_BLOCK_START = 8*1024*1024;
//    public static final int MAP_BLOCK_SIZE = 32*1024*1024;
    public static final int ARRAY_SIZE = (int) (READ_BLOCK_SIZE / 40 * 1.2);
    public static final int bufferSize = WRITE_BUFFER_SIZE / 7;
//-    public static final int FREESIZE = 100;
    public static final double FlushRatio = 1.5;
    public static final int FREESIZE = 36;

    public static ExecutorService executorService = Executors.newFixedThreadPool(8);
    public ConcurrentLinkedQueue<BucketFile> arrayBlockingQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<BucketFile> nextArrayBlockingQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<ReadBucket> bytesArrayBlockingQueue = new ConcurrentLinkedQueue<>();

    public ConcurrentLinkedQueue<long[]> arrayLongQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<long[]> partArrayLongQueue = new ConcurrentLinkedQueue<>();

    public ConcurrentLinkedQueue<ReadBucket> freeReadLineQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<ReadBucket> freeReadOrderQueue = new ConcurrentLinkedQueue<>();

    public ConcurrentLinkedQueue<BucketFile> freeBufferQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<ByteBuffer> freeBytesQueue = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<long[]> freeLongQueue = new ConcurrentLinkedQueue<>();

    private final AtomicInteger first = new AtomicInteger(1);
    public DiskQueryEngine orderQueryEngine;
    public DiskQueryEngine partQueryEngine;
    public File partMeta;
    public File orderMeta;
    public long[][] orderKeyPosition = new long[3][PARTITION * 2];
    public long[][] partKeyPosition = new long[3][PARTITION * 2];
    public long[] orderPosition = new long[PARTITION * 2];
    public long[] partPosition = new long[PARTITION * 2];

    public int orderIdx = 0;
    public int partIdx = 0;

    public AtomicLong readIndex = new AtomicLong(0);
    public AtomicLong readIndex2 = new AtomicLong(0);
    public AtomicInteger writeCount = new AtomicInteger(0);

    public AtomicLong orderHeadIndex = new AtomicLong(0);
    public AtomicLong orderBottomIndex = new AtomicLong(0);
    public AtomicLong partHeadIndex = new AtomicLong(0);
    public AtomicLong partBottomIndex = new AtomicLong(0);

    int orderTableID = 1;
    int lineTableID = 0;

    long blockStart = 7 * 100_0000;
//    public ConcurrentController cc = new ConcurrentController();

//    public static final BucketFile[] flushOrderArray = new BucketFile[PARTITION];
//    public static final BucketFile[] flushPartArray = new BucketFile[PARTITION];

    /**
     * The implementation must contain a public no-argument constructor.
     */
    public RaceAnalyticDB() {
        try {
            orderMeta = new File("/adb-data/player/order_meta");
            if (!orderMeta.exists()) {
                orderMeta.createNewFile();
            }

            partMeta = new File("/adb-data/player/part_meta");
            if (!partMeta.exists()) {
                partMeta.createNewFile();
            } else {
                initPosition("/adb-data/player");

//                long s = 0;
//                long f = 0;
//                for (int i = 0; i < PARTITION; i++) {
//                    f += partKeyPosition[i];
//                    s += partKeyPosition[PARTITION + i] - partKeyPosition[i];
//                    System.out.println("endPosition:" + partKeyPosition[PARTITION + i] + " startPosition" + partKeyPosition[i] + " totalSize:" + s + " first totalSize:" + f);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        new Thread(()-> {
            try {
                Thread.sleep(50*1000);
                System.exit(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

//    @Override
//    public void bload(String tpchDataFileDir, String workspaceDir) throws Exception {
//        File dir = new File(tpchDataFileDir);
//        new Thread(VmstatLogger::useLinuxCommond2).start();
//        final int threadNum = 8;
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
//
////                    new Thread(() -> {
////                        for (int i = 0; i < FREESIZE * 4; i++) {
////                            ReadBucket rb = new ReadBucket();
////                            rb.table = "line";
////                            freeReadLineQueue.add(rb);
////                        }
////                    }).start();
//
////                    new Thread(() -> {
////                        for (int i = 0; i < FREESIZE * 4; i++) {
////                            ReadBucket rb = new ReadBucket();
////                            rb.table = "order";
////                            freeReadOrderQueue.add(rb);
////                        }
////                    }).start();
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
//                            for (int i = 0; i < PARTITION * DISPATCH_THREAD_NUM * 4; i++) {
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
//                // 设置对齐的 n 个片
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
//                CountDownLatch sourceDownLatch = new CountDownLatch(8);
//
//                for (int k = 0; k < 8; k++) {
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
//                                int size = (int) Math.min(READ_BLOCK_SIZE, partitionTotalSize - readPosition);
////                                byteBuffer.clear();
//                                long s = System.currentTimeMillis();
//                                fileChannel.read(byteBuffer, readPosition);
////                                MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, readPosition, 64 * 1024 * 1024);
//
//                                pollCost += System.currentTimeMillis() - s;
//
//                                int l = size;
//                                while (l > 0) {
//                                    if (unsafe.getByte(address + l - 1) == '\n') {
////                                        byteBuffer.limit(l);
//                                        break;
//                                    }
//                                    l--;
//                                }
//
//                                long val = 0;
//                                for (int i = 0; i < l; i++) {
//                                    byte temp = unsafe.getByte(address+i);
//                                    do {
//                                        val = val * 10 + (temp - '0');
//                                        temp = unsafe.getByte(address+(++i));
//                                    } while (temp != ',');
//
//                                    val = 0;
//                                    // skip ，
//                                    i++;
//                                    temp = unsafe.getByte(address+i);
//                                    do {
//                                        val = val * 10 + (temp - '0');
//                                        temp =unsafe.getByte(address+ (++i));
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

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        if (orderMeta.length() > 0 && partMeta.length() > 0) {
            return;
        }
//        File dir = new File(tpchDataFileDir);
//        new Thread(VmstatLogger::useLinuxCommond2).start();
        DiskRaceEngine orderRaceEngine = new DiskRaceEngine();
        DiskRaceEngine partRaceEngine = new DiskRaceEngine();
        long sss = System.currentTimeMillis();
        ConcurrentController cc = new ConcurrentController();
//                String col = dataFile.getName().equals("lineitem")?"L_ORDERKEY":"O_ORDERKEY";
//                String orderKey = tableColumnKey(dataFile.getName(), col);
//                orderQueryEngine = new DiskQueryEngine(workspaceDir, orderKey);
//                columnName2EngineMap.put(orderKey, orderQueryEngine);
//                col = dataFile.getName().equals("lineitem")?"L_PARTKEY":"O_CUSTKEY";
//                String partKey = tableColumnKey(dataFile.getName(), col);
//                partQueryEngine = new DiskQueryEngine(workspaceDir, partKey);
//                columnName2EngineMap.put(partKey, partQueryEngine);



//                Future[] fs = new Future[2];
//                fs[0] = executorService.submit(() -> {
//                    String col = "ORDERKEY";
//                    String orderKey = tableColumnKey(dataFile.getName(), col);
//                    orderQueryEngine = new DiskQueryEngine(workspaceDir, orderKey);
//                    columnName2EngineMap.put(orderKey, orderQueryEngine);
//                    partQueryEngine.init(0);
//                });
//
//                fs[1] = executorService.submit(() -> {
//                    String col = "PARTKEY";
//                    String partKey = tableColumnKey(dataFile.getName(), col);
//                    partQueryEngine = new DiskQueryEngine(workspaceDir, partKey);
//                    columnName2EngineMap.put(partKey, partQueryEngine);
//                    orderQueryEngine.init(0);
//                });
//
//                for (Future f : fs) {
//                    f.get();
//                }

//                int a = 0;
//                File n = new File(workspaceDir + File.separator + dataFile.getName() + "_recovery");
//                if (n.exists()) {
//                    a = 1;
//                }
//
//                if (a == 1) {
//                    continue;
//                }



        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeBytesQueue.add(ByteBuffer.allocateDirect(READ_BLOCK_SIZE));
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeBytesQueue.add(ByteBuffer.allocateDirect(READ_BLOCK_SIZE));
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeLongQueue.add(new long[ARRAY_SIZE]);
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeLongQueue.add(new long[ARRAY_SIZE]);
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeLongQueue.add(new long[ARRAY_SIZE]);
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < FREESIZE; i++) {
                freeLongQueue.add(new long[ARRAY_SIZE]);
            }
        }).start();

        Future[] fs = new Future[10];

        final String col = "ORDERKEY";
        orderQueryEngine = new DiskQueryEngine(workspaceDir, col);
        fs[0] = executorService.submit(() -> {
            for (int j = 0; j < 1024; j++) {
                orderQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col + "_0_" + j);
            }
        });
        fs[1] = executorService.submit(() -> {
            for (int j = 1024; j < PARTITION; j++) {
                orderQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col + "_0_" + j);
            }
        });

        final String col2 = "PARTKEY";
        partQueryEngine = new DiskQueryEngine(workspaceDir, col2);
        fs[2] = executorService.submit(() -> {
            for (int j = 0; j < 1024; j++) {
                partQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col2 + "_0_" + j);
            }
        });

        fs[3] = executorService.submit(() -> {
            for (int j = 1024; j < PARTITION; j++) {
                partQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col2 + "_0_" + j);
            }
        });

        for (int futureNo = 4; futureNo < 8; futureNo++) {
            fs[futureNo] = executorService.submit(() -> {
                for (int i = 0; i < PARTITION * 2; i++) {
                    freeBufferQueue.add(new BucketFile(ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE)));
                }
            });
        }

//                    fs[8] = executorService.submit(() -> {
//                        for (int i = 0; i < PARTITION; i++) {
//                            flushOrderArray[i] = new BucketFile(ByteBuffer.allocateDirect(FLUSH_BUFFER_SIZE));
//                        }
//                    });
//                    fs[9] = executorService.submit(() -> {
//                        for (int i = 0; i < PARTITION; i++) {
//                            flushPartArray[i] = new BucketFile(ByteBuffer.allocateDirect(FLUSH_BUFFER_SIZE));
//                        }
//                    });

        fs[8] = executorService.submit(() -> {
            for (int i = 0; i < FREESIZE*10; i++) {
                ReadBucket rb = new ReadBucket();
                freeReadLineQueue.add(rb);
            }
        });
        fs[9] = executorService.submit(() -> {
            for (int i = 0; i < FREESIZE*10; i++) {
                ReadBucket rb = new ReadBucket();
                freeReadOrderQueue.add(rb);
            }
        });

        for (Future f : fs) {
            f.get();
        }

        columnName2EngineMap.put(col, orderQueryEngine);
        columnName2EngineMap.put(col2, partQueryEngine);

        System.out.println(freeBufferQueue.size() + " " + freeLongQueue.size() + " " + freeBytesQueue.size() + " ");

        long start = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File(tpchDataFileDir + File.separator + "lineitem"), "r");
        FileChannel fileChannel = randomAccessFile.getChannel();
        long totalSize = fileChannel.size();

        RandomAccessFile randomAccessFile2 = new RandomAccessFile(new File(tpchDataFileDir + File.separator + "orders"), "r");
        FileChannel fileChannel2 = randomAccessFile2.getChannel();
        long totalSize2 = fileChannel2.size();
//                long[] readThreadPosition = new long[threadNum];
//                // 设置对齐的 n 个片
//                readThreadPosition[0] = 21;
//                int preReadSize = 50;
//                for (int i = 1; i < threadNum; i++) {
//                    long paddingPosition = totalSize / threadNum * i;
//                    MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, paddingPosition, preReadSize);
//                    for (int j = 0; j < preReadSize; j++) {
//                        if (mappedByteBuffer.get() == (byte)'\n') {
//                            paddingPosition += j + 1;
//                            break;
//                        }
//                    }
//                    readThreadPosition[i] = paddingPosition;
//                }

//                CyclicBarrier readBarrier = new CyclicBarrier(READ_THREAD_NUM);
//                CountDownLatch sourceDownLatch = new CountDownLatch(READ_THREAD_NUM);
//                CountDownLatch readDownLatch = new CountDownLatch(2*WRITE_THREAD_NUM);
//                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
//                CountDownLatch dispatchLatch = new CountDownLatch(DISPATCH_THREAD_NUM*2);

        for (int k = 0; k < READ_THREAD_NUM/2; k++) {
            final int threadNo = k;
            new Thread(() -> {
                long pollCost = 0;
                long readFileCost = 0;
                ReadBucket readbucket = freeReadOrderQueue.poll();
                int iii = 0;
                long threadStart = System.currentTimeMillis();
                try {
                    // int readBufferSize = READ_BLOCK_SIZE;
                    ByteBuffer byteBuffer = freeBytesQueue.poll();
                    while (byteBuffer == null) {
                        Thread.sleep(100);
                        byteBuffer = freeBytesQueue.poll();
                    }

                    while (true) {
                        long idx = readIndex2.getAndIncrement();
                        long readPosition = READ_BLOCK_START * idx;
                        if (readPosition > totalSize2) {
                            break;
                        }

//                        if (writeCount.get() > 3 && threadNo == 0) {
//                            executorService.execute(() -> {
//                                ByteBuffer newBuffer = freeBytesQueue.poll();
//                                while (newBuffer == null) {
//                                    try {
//                                        Thread.sleep(100);
//                                    } catch (InterruptedException e) {
//                                        e.printStackTrace();
//                                    }
//                                    newBuffer = freeBytesQueue.poll();
//                                }
//                                long newIdx = readIndex2.getAndIncrement();
//                                long newReadPosition = READ_BLOCK_START * newIdx;
//                                if (newReadPosition > totalSize2) {
//                                    return;
//                                }
//
//                                int size = (int)Math.min(READ_BLOCK_SIZE, totalSize2 - newReadPosition);
//                                newBuffer.clear();
//                                try {
//                                    fileChannel2.read(newBuffer, newReadPosition);
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
//                                long address = ((DirectBuffer) newBuffer).address();
//                                while (size > 0) {
//                                    if (unsafe.getByte(address + size - 1) == 10) {
//                                        newBuffer.limit(size);
//                                        break;
//                                    }
//                                    size--;
//                                }
//                                bytesArrayBlockingQueue.add(newBuffer);
//                            });
//                        }

                        int size = (int)Math.min(READ_BLOCK_SIZE, totalSize2 - readPosition);
                        byteBuffer.clear();
                        fileChannel2.read(byteBuffer, readPosition);
                        long address = ((DirectBuffer) byteBuffer).address();
                        while (size > 0) {
                            if (unsafe.getByte(address + size - 1) == 10) {
                                byteBuffer.limit(size);
                                break;
                            }
                            size--;
                        }

                        ReadBucket tmpRead = readbucket;
                        tmpRead.byteBuffer = byteBuffer;
                        tmpRead.tableID = orderTableID;
                        bytesArrayBlockingQueue.add(tmpRead);

                        readbucket = freeReadOrderQueue.poll();
                        while (readbucket == null) {
                            Thread.sleep(100);
                            readbucket = freeReadOrderQueue.poll();
                        }

                        byteBuffer = freeBytesQueue.poll();
                        while (byteBuffer == null) {
                            Thread.sleep(100);
                            byteBuffer = freeBytesQueue.poll();
                        }
                    }

//                            long readPosition = readThreadPosition[threadNo];
//                            long partitionTotalSize;
//                            if (threadNo == READ_THREAD_NUM - 1) {
//                                partitionTotalSize = totalSize;
//                            } else {
//                                partitionTotalSize = readThreadPosition[threadNo + 1];
//                            }

//                            while (readPosition < partitionTotalSize - 1) {
//                                int size = (int)Math.min(READ_BLOCK_SIZE, partitionTotalSize - readPosition);
//                                byteBuffer.clear();
////                                long s = System.currentTimeMillis();
//                                fileChannel.read(byteBuffer, readPosition);
////                                pollCost += System.currentTimeMillis() - s;
//                                long address = ((DirectBuffer) byteBuffer).address();
//                                while (size > 0) {
//                                    if (unsafe.getByte(address + size - 1) == '\n') {
//                                        byteBuffer.limit(size);
//                                        break;
//                                    }
//                                    size--;
//                                }
//
//                                readPosition += size;
//                                ByteBuffer tmp = byteBuffer;
//                                bytesArrayBlockingQueue.add(tmp);
//                                byteBuffer = freeBytesQueue.poll();
//                                while (byteBuffer == null) {
//                                    Thread.sleep(100);
//                                    byteBuffer = freeBytesQueue.poll();
//                                }
                    // System.out.println(System.currentTimeMillis() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size());
//                            }
                    freeBytesQueue.add(byteBuffer);
//                            cc.readBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //                        System.out.println(Thread.currentThread().getName() + " finish read");
                cc.sourceDownLatch.countDown();
                System.out.println("read thread cost " + (System.currentTimeMillis() - threadStart) + " ms " + pollCost + "ms" + readFileCost + " ms");
            }).start();
        }

        for (int k = 0; k < READ_THREAD_NUM/2; k++) {
            final int threadNo = k;
            new Thread(() -> {
                long pollCost = 0;
                long readFileCost = 0;
                ReadBucket readbucket = freeReadLineQueue.poll();
                long threadStart = System.currentTimeMillis();
                try {
                    // int readBufferSize = READ_BLOCK_SIZE;
                    ByteBuffer byteBuffer = freeBytesQueue.poll();
                    while (byteBuffer == null) {
                        Thread.sleep(100);
                        byteBuffer = freeBytesQueue.poll();
                    }

                    while (true) {
                        long idx = readIndex.getAndIncrement();
                        long readPosition = READ_BLOCK_START * idx;
                        if (readPosition > totalSize) {
                            break;
                        }

//                        if (writeCount.get() > 3 && threadNo == 0) {
//                            executorService.execute(() -> {
//                                ByteBuffer newBuffer = freeBytesQueue.poll();
//                                while (newBuffer == null) {
//                                    try {
//                                        Thread.sleep(100);
//                                    } catch (InterruptedException e) {
//                                        e.printStackTrace();
//                                    }
//                                    newBuffer = freeBytesQueue.poll();
//                                }
//                                long newIdx = readIndex.getAndIncrement();
//                                long newReadPosition = READ_BLOCK_START * newIdx;
//                                if (newReadPosition > totalSize) {
//                                    return;
//                                }
//
//                                int size = (int)Math.min(READ_BLOCK_SIZE, totalSize - newReadPosition);
//                                newBuffer.clear();
//                                try {
//                                    fileChannel.read(newBuffer, newReadPosition);
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
//                                long address = ((DirectBuffer) newBuffer).address();
//                                while (size > 0) {
//                                    if (unsafe.getByte(address + size - 1) == 10) {
//                                        newBuffer.limit(size);
//                                        break;
//                                    }
//                                    size--;
//                                }
//                                bytesArrayBlockingQueue.add(newBuffer);
//                            });
//                        }

                        int size = (int)Math.min(READ_BLOCK_SIZE, totalSize - readPosition);
                        byteBuffer.clear();
                        fileChannel.read(byteBuffer, readPosition);
                        long address = ((DirectBuffer) byteBuffer).address();
                        while (size > 0) {
                            if (unsafe.getByte(address + size - 1) == 10) {
                                byteBuffer.limit(size);
                                break;
                            }
                            size--;
                        }

//                        ByteBuffer tmp = byteBuffer;
//                        bytesArrayBlockingQueue.add(tmp);


                        ReadBucket tmpRead = readbucket;
                        tmpRead.byteBuffer = byteBuffer;
                        tmpRead.tableID = lineTableID;
                        bytesArrayBlockingQueue.add(tmpRead);

                        readbucket = freeReadLineQueue.poll();
                        while (readbucket == null) {
                            Thread.sleep(100);
                            readbucket = freeReadLineQueue.poll();
                        }

                        byteBuffer = freeBytesQueue.poll();
                        while (byteBuffer == null) {
                            Thread.sleep(100);
                            byteBuffer = freeBytesQueue.poll();
                        }
                    }

//                            long readPosition = readThreadPosition[threadNo];
//                            long partitionTotalSize;
//                            if (threadNo == READ_THREAD_NUM - 1) {
//                                partitionTotalSize = totalSize;
//                            } else {
//                                partitionTotalSize = readThreadPosition[threadNo + 1];
//                            }

//                            while (readPosition < partitionTotalSize - 1) {
//                                int size = (int)Math.min(READ_BLOCK_SIZE, partitionTotalSize - readPosition);
//                                byteBuffer.clear();
////                                long s = System.currentTimeMillis();
//                                fileChannel.read(byteBuffer, readPosition);
////                                pollCost += System.currentTimeMillis() - s;
//                                long address = ((DirectBuffer) byteBuffer).address();
//                                while (size > 0) {
//                                    if (unsafe.getByte(address + size - 1) == '\n') {
//                                        byteBuffer.limit(size);
//                                        break;
//                                    }
//                                    size--;
//                                }
//
//                                readPosition += size;
//                                ByteBuffer tmp = byteBuffer;
//                                bytesArrayBlockingQueue.add(tmp);
//                                byteBuffer = freeBytesQueue.poll();
//                                while (byteBuffer == null) {
//                                    Thread.sleep(100);
//                                    byteBuffer = freeBytesQueue.poll();
//                                }
                    // System.out.println(System.currentTimeMillis() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size());
//                            }
                    freeBytesQueue.add(byteBuffer);
//                            cc.readBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //                        System.out.println(Thread.currentThread().getName() + " finish read");
                cc.sourceDownLatch.countDown();
                System.out.println("read thread cost " + (System.currentTimeMillis() - threadStart) + " ms " + pollCost + "ms" + readFileCost + " ms");
            }).start();
        }

        for (int k = 0; k < THREAD_NUM; k++) {
            // final int threadNo = k;
            new Thread(() -> {
                long threadStart = System.currentTimeMillis();
                long pollCount1 = 0;
                long pollCount2 = 0;
                //                        long pollCost = 0;
                long pollCost1 = 0;
                long pollCost2 = 0;
                int No;
                int size;
                long val;
                byte[] tmpBytes = new byte[19];
                long address;
                ByteBuffer readBufferArray;
                ReadBucket readBucket;
                //                        long pollCount = 0;
                try {
                    while(true) {
                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
                        readBucket = bytesArrayBlockingQueue.poll();
                        // long readCostStart = System.currentTimeMillis();
                        while (readBucket == null) {
                            //pollCount++;
                            Thread.sleep(100);
                            readBucket = bytesArrayBlockingQueue.poll();
                        }
                        // readPollCost += System.currentTimeMillis() - readCostStart;
                        readBufferArray = readBucket.byteBuffer;

                        size = readBufferArray.limit();
                        if (size == 0) {
                            break;
                        }

                        long[] orderLong = freeLongQueue.poll();
                        // long o1Start = System.currentTimeMillis();
                        while (orderLong == null) {
                            Thread.sleep(100);
                            orderLong = freeLongQueue.poll();
                        }
                        // pollCost1 += System.currentTimeMillis() - o1Start;

                        long[] partLong = freeLongQueue.poll();
                        // o1Start = System.currentTimeMillis();
                        while (partLong == null) {
                            Thread.sleep(100);
                            partLong = freeLongQueue.poll();
                        }
                        // pollCost2 += System.currentTimeMillis() - o1Start;


                        No = 0;
                        val = 0;
                        address = ((DirectBuffer) readBufferArray).address();
                        int i = RRE_SIZE;
                        while (unsafe.getByte(address+i-1) != 10) {
                            i--;
                        }

                        for (; i < size; i++) {
                            byte temp = unsafe.getByte(address+i);
                            if (unsafe.getByte(address+i+18) == 44) {
                                unsafe.copyMemory(null, address+i+1, tmpBytes, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, 18);
                                for (int innerIdx = 0; innerIdx < 18; innerIdx++) {
                                    val = val * 10 + (temp & (0x0f));
//                                            temp = unsafe.getByte(address+i+(innerIdx));
                                    temp = tmpBytes[innerIdx];
                                }
                                i+=18;
                            } else {
                                do {
                                    val = val * 10 + (temp & (0x0f));
                                    temp = unsafe.getByte(address+(++i));
                                } while (temp != ',');
                            }
                            unsafe.putLong(orderLong, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + No*8L, val);
//                                    orderLong[No] = val;
                            val = 0;
                            // skip ，
                            i++;
                            temp = unsafe.getByte(address+i);
                            if (unsafe.getByte(address+i+18) == 10) {
                                unsafe.copyMemory(null, address+i+1, tmpBytes, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, 18);
                                for (int innerIdx = 0; innerIdx < 18; innerIdx++) {
                                    val = val * 10 + (temp & (0x0f));
//                                            temp = unsafe.getByte(address+i+(innerIdx));
                                    temp = tmpBytes[innerIdx];
                                }
                                i+=18;
                            } else {
                                do {
                                    val = val * 10 + (temp & (0x0f));
                                    temp = unsafe.getByte(address+(++i));
                                } while (temp != '\n');
                            }
                            unsafe.putLong(partLong, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (No++)*8L, val);
//                                    partLong[No++] = val;
                            val = 0;
                            // skip \n
                        }

                        freeBytesQueue.add(readBufferArray);
                        if (readBucket.tableID == orderTableID) {
                            freeReadOrderQueue.add(readBucket);
                            orderLong[orderLong.length-2] = orderTableID;
                            partLong[partLong.length-2] = orderTableID;
                        } else {
                            freeReadLineQueue.add(readBucket);
                            orderLong[orderLong.length-2] = lineTableID;
                            partLong[partLong.length-2] = lineTableID;
                        }

                        orderLong[orderLong.length-1] = No;
                        partLong[orderLong.length-1] = No;

                        long[] tmp = orderLong;
                        arrayLongQueue.add(tmp);
                        long[] tmpPart = partLong;
                        partArrayLongQueue.add(tmpPart);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                cc.countDownLatch.countDown();
                //                        System.out.println("parse thread cost " + (System.currentTimeMillis() - threadStart) + " ms" + pollCount + " times " + pollCost + " ms" + pollCost1 + " ms" + pollCost2 + " ms" + pollCost3 + " ms" + pollCost4 + " ms" + pollCost5 + " ms" + pollCost6 + " ms" + pollCost7 + " ms");
                //                        System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
                //                        System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
                System.out.println("parse thread cost " + (System.currentTimeMillis() - threadStart) + " " + pollCount1 + " " + pollCost1 + " " + pollCount2 + " " + pollCost2);
            }).start();
        }

        for (int k = 0; k < DISPATCH_THREAD_NUM; k++) {
//                    final int threadNo = k;
            new Thread(() -> {
                Thread.currentThread().setPriority(9);
                long threadStart = System.currentTimeMillis();
                long pollCount = 0;
                long pollCost = 0;
                for (int j = 0; j < 2; j++) {
                    for (int i = 0; i < PARTITION; i++) {
                        if (orderRaceEngine.bucketFiles[j][i] == null) {
                            orderRaceEngine.bucketFiles[j][i] = freeBufferQueue.poll();
                        }
                    }
                }
                long[] toBeParsedArray;
                long[][] prePartitionArray = new long[16][ARRAY_SIZE/15];
                int[] preIndex = new int[16];
                int size;
                long value;
                int partition;
                int tableIdx;
                BucketFile[] bs;
                try {
                    while(true) {
                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
                        // long ss = System.currentTimeMillis();
                        toBeParsedArray = arrayLongQueue.poll();
                        while (toBeParsedArray == null) {
                            // pollCount++;
                            Thread.sleep(100);
                            toBeParsedArray = arrayLongQueue.poll();
                        }
                        // pollCost += System.currentTimeMillis() - ss;

                        size = toBeParsedArray.length;
                        if (size == 0) {
                            break;
                        }

                        // long dispatchTime = System.currentTimeMillis();
                        BucketFile b;
                        int idx = 0;
                        long bound = toBeParsedArray[size - 1];

                        do {
//                                    value = toBeParsedArray[idx++];
                            value = unsafe.getLong(toBeParsedArray, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (idx++)*8L);
                            partition = (int) (value >> 59);
                            prePartitionArray[partition][preIndex[partition]++] = value;
//                                    b = orderRaceEngine.bucketFiles[partition];
//                                    unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
                        } while (idx < bound);

                        long tableID = toBeParsedArray[size - 2];
                        tableIdx = (int) tableID;
                        bs = orderRaceEngine.bucketFiles[tableIdx];
                        for (int i = 0; i < 16; i++) {
                            long[] aaa = prePartitionArray[i];
                            for (int innerIdx = 0; innerIdx < preIndex[i]; innerIdx++) {
                                value = aaa[innerIdx];
                                partition = (int) (value >> (MOVED));
                                b = bs[partition];
                                unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
                            }
                            preIndex[i] = 0;
                        }

                        for (int p = 0; p < PARTITION; p++) {
                            b = bs[p];
                            if (bufferSize - b.bufferIndex <  FlushRatio * ARRAY_SIZE / PARTITION) {
                                BucketFile tmp = b;
                                tmp.partitionNo = p;
                                tmp.tableID = b.tableID;
                                arrayBlockingQueue.add(tmp);
                                b = freeBufferQueue.poll();
                                while (b == null) {
                                    Thread.sleep(100);
                                    b = freeBufferQueue.poll();
                                }
                                b.tableID = tableIdx;
                                orderRaceEngine.bucketFiles[tableIdx][p] = b;
                            }
                        }

                        freeLongQueue.add(toBeParsedArray);
                        // dispatchCost += System.currentTimeMillis() - dispatchTime;
                    }

                    for (int i = 0; i < 2; i++) {
                        int flushNo = 0;
                        BucketFile[] bf = orderRaceEngine.bucketFiles[i];
                        do {
                            BucketFile b = bf[flushNo];

                            b.partitionNo = flushNo++;
                            b.tableID = i;
                            arrayBlockingQueue.add(b);
//                            orderRaceEngine.bucketFiles[i][flushNo++] = null;
                        } while (flushNo < PARTITION);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                cc.dispatchLatch.countDown();
                System.out.println("dispatch thread one cost " + (System.currentTimeMillis() - threadStart) + " ms " + System.currentTimeMillis() + " " + pollCount + " " + pollCost);
                // System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
                // System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
            }).start();
        }

        for (int k = 0; k < DISPATCH_THREAD_NUM; k++) {
//                    final int threadNo = k;
            new Thread(() -> {
                long threadStart = System.currentTimeMillis();
                long pollCount = 0;
                long pollCost = 0;
                Thread.currentThread().setPriority(9);
                for (int j = 0; j < 2; j++) {
                    for (int i = 0; i < PARTITION; i++) {
                        if (partRaceEngine.bucketFiles[j][i] == null) {
                            partRaceEngine.bucketFiles[j][i] = freeBufferQueue.poll();
                        }
                    }
                }
                long[] toBeParsedArray;
                long[][] prePartitionArray = new long[16][ARRAY_SIZE/15];
                int[] preIndex = new int[16];
                BucketFile[] bs;
                int size;
                long value;
                int partition;
                int tableIdx;
                try {
                    while(true) {
                        // System.out.println(arrayBlockingQueue.size() + " " + nextArrayBlockingQueue.size() + " " + bytesArrayBlockingQueue.size() + " " + freeBytesQueue.size() + " ");
                        toBeParsedArray = partArrayLongQueue.poll();
                        // long ss = System.currentTimeMillis();
                        while (toBeParsedArray == null) {
                            Thread.sleep(100);
                            // pollCount++;
                            toBeParsedArray = partArrayLongQueue.poll();
                        }
                        // pollCost+= System.currentTimeMillis() - ss;

                        size = toBeParsedArray.length;
                        if (size == 0) {
                            break;
                        }

                        // long dispatchTime = System.currentTimeMillis();

                        BucketFile b;
                        int idx = 0;
                        long bound = toBeParsedArray[size - 1];

                        do {
//                                    value = toBeParsedArray[idx++];
                            value = unsafe.getLong(toBeParsedArray, UnsafeUtil.LONG_ARRAY_BASE_OFFSET + (idx++)*8L);
                            partition = (int) (value >> 59);
                            prePartitionArray[partition][preIndex[partition]++] = value;
//                                    b = orderRaceEngine.bucketFiles[partition];
//                                    unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
                        } while (idx < bound);
                        long tableID = toBeParsedArray[size - 2];
                        tableIdx = tableID == lineTableID?0:1;
                        if (tableIdx == lineTableID) {
                            pollCost += bound;
                        } else {
                            pollCount += bound;
                        }
                        bs = partRaceEngine.bucketFiles[tableIdx];
                        for (int i = 0; i < 16; i++) {
                            long[] aaa = prePartitionArray[i];
                            for (int innerIdx = 0; innerIdx<preIndex[i]; innerIdx++) {
                                value = aaa[innerIdx];
                                partition = (int) (value >> (MOVED));
                                b = bs[partition];
                                unsafe.putLong(b.address + (b.bufferIndex++) * 7L, value);
                            }
                            preIndex[i] = 0;
                        }

                        for (int p = 0; p < PARTITION; p++) {
                            b = bs[p];
                            if (bufferSize - b.bufferIndex < FlushRatio * ARRAY_SIZE / PARTITION) {
                                BucketFile tmp = b;
                                tmp.partitionNo = p;
                                tmp.tableID = b.tableID;
                                nextArrayBlockingQueue.add(tmp);
                                b = freeBufferQueue.poll();
                                while (b == null) {
                                    Thread.sleep(100);
                                    b = freeBufferQueue.poll();
                                }
                                b.tableID = tableIdx;
                                partRaceEngine.bucketFiles[tableIdx][p] = b;
                            }
                        }

                        // dispatchCost += System.currentTimeMillis() - dispatchTime;
                        freeLongQueue.add(toBeParsedArray);
                    }

                    for (int i = 0; i < 2; i++) {
                        int flushNo = 0;
                        do {
                            BucketFile b = partRaceEngine.bucketFiles[i][flushNo];
                            b.partitionNo = flushNo++;
                            b.tableID = i;
                            nextArrayBlockingQueue.add(b);
//                            partRaceEngine.bucketFiles[i][flushNo++] = null;
                        } while (flushNo < PARTITION);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                cc.dispatchLatch.countDown();
                // System.out.println("dispatch thread two cost " + (System.currentTimeMillis() - threadStart) + " ms" + System.currentTimeMillis());
                System.out.println("dispatch thread two cost " + (System.currentTimeMillis() - threadStart) + " ms " + System.currentTimeMillis() + " " + pollCount + " " + " " + pollCost);
                // System.out.println("parse thread poll " + partRaceEngine.pollCount + orderRaceEngine.pollCount + " times");
                // System.out.println("parse theard cost" + parseCost + " dispatch cost" + dispatchCost);
            }).start();
        }

        for (int k = 0; k < WRITE_THREAD_NUM; k++) {
            final int threadNo = k;
            new Thread(() -> {
                long writeCost = 0;
                long pollCost = 0;
                long pollCount = 0;
                long position = 0;
                long buf = 0;
                long threadStart = System.currentTimeMillis();
                try {
                    while(true) {
                        // long costStart = System.currentTimeMillis();
                        BucketFile orderBucket = arrayBlockingQueue.poll();
                        while (orderBucket == null) {
//                                    pollCount++;
                            Thread.sleep(100);
                            orderBucket = arrayBlockingQueue.poll();
                        }
                        // pollCost += System.currentTimeMillis() - costStart;

                        if (orderBucket.destroy()) {
                            break;
                        }

                        int partitionNo = orderBucket.partitionNo;
                        long delta = orderBucket.bufferIndex*7L;
                        if (orderBucket.tableID == orderTableID) {
                            buf = orderBottomIndex.getAndAdd(delta);
                            orderKeyPosition[threadNo][partitionNo+PARTITION] += delta;
                            position = blockStart;
                        }  else if (orderBucket.tableID == lineTableID) {
                            buf = orderHeadIndex.getAndAdd(delta);
                            orderKeyPosition[threadNo][partitionNo] += delta;
                            position = 0;
                        }

                        FileWriter fw = orderQueryEngine.fileWriters[partitionNo];
                        orderBucket.byteBuffer.flip();
                        orderBucket.byteBuffer.limit((int) delta);
//                        writeCount.incrementAndGet();
//                        long buf = fw.writePosition.getAndAdd(orderBucket.bufferIndex);
                        fw.fileChannel.write(orderBucket.byteBuffer, position + buf);
//                        writeCount.decrementAndGet();
//                        fw.writePosition.set(buf);
                        orderBucket.bufferIndex = 0;
                        orderBucket.tableID = -1;
                        freeBufferQueue.add(orderBucket);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                cc.readDownLatch.countDown();
                System.out.println("first write thread cost " + (System.currentTimeMillis() - threadStart) + " ms writeCost" + writeCost + " ms" + pollCost + " ms " + System.currentTimeMillis());
//                        System.out.println("parse thread poll " + pollCount + " times");
            }).start();

            new Thread(() -> {
                long threadStart = System.currentTimeMillis();
                long writeCost = 0;
                long pollCost = 0;
                long pollCount = 0;
                long position = 0;
                long buf = 0;
                try {
                    while (true) {
                        // long costStart = System.currentTimeMillis();
                        BucketFile partBucket = nextArrayBlockingQueue.poll();
                        while (partBucket == null) {
//                                     pollCount++;
                            Thread.sleep(100);
                            partBucket = nextArrayBlockingQueue.poll();
                        }
                        // pollCost += System.currentTimeMillis() - costStart;

                        if (partBucket.destroy()) {
                            break;
                        }

                        // long writeStart = System.currentTimeMillis();
                        long delta = partBucket.bufferIndex*7L;
                        if (partBucket.tableID == orderTableID) {
                            buf = partBottomIndex.getAndAdd(delta);
                            partKeyPosition[threadNo][partBucket.partitionNo+PARTITION] += delta;
                            position = blockStart;
                        } else if (partBucket.tableID == lineTableID) {
                            buf = partHeadIndex.getAndAdd(delta);
                            partKeyPosition[threadNo][partBucket.partitionNo] += delta;
                            position = 0;
                        }

                        FileWriter fw = partQueryEngine.fileWriters[partBucket.partitionNo];
                        partBucket.byteBuffer.flip();
                        partBucket.byteBuffer.limit((int) delta);
//                        writeCount.incrementAndGet();
//                        long buf = fw.writePosition.getAndAdd(partBucket.bufferIndex);
                        fw.fileChannel.write(partBucket.byteBuffer, position + buf);
//                        writeCount.decrementAndGet();
//                        fw.writePosition.set(buf);
                        partBucket.bufferIndex = 0;
                        partBucket.tableID = -1;
                        freeBufferQueue.add(partBucket);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                cc.readDownLatch.countDown();
//                        System.out.println("parse thread poll " + pollCount + " times");
                System.out.println("second write thread cost " + (System.currentTimeMillis() - threadStart) + " ms, writeCost:" + writeCost + " ms " + pollCost + " ms " + System.currentTimeMillis());
            }).start();
        }
        cc.sourceDownLatch.await();

        for (int i = 0; i < THREAD_NUM; i++) {
            ReadBucket b = new ReadBucket();
            b.byteBuffer = ByteBuffer.allocateDirect(0);
            bytesArrayBlockingQueue.add(b);
        }
        cc.countDownLatch.await();

        for (int i = 0; i < DISPATCH_THREAD_NUM; i++) {
            arrayLongQueue.add(new long[0]);
            partArrayLongQueue.add(new long[0]);
        }

        cc.dispatchLatch.await();

        // todo 循环展开
        for (int i = 0; i < WRITE_THREAD_NUM; i++) {
            arrayBlockingQueue.add(new BucketFile(true));
            nextArrayBlockingQueue.add(new BucketFile(true));
        }

        cc.readDownLatch.await();

//                Future[] fs = new Future[2];
//
//                fs[0] = executorService.submit(() -> {
//                    int flushNo = 0;
//                    do {
//                        BucketFile b = flushOrderArray[flushNo];
//                        FileWriter fw = orderQueryEngine.fileWriters[flushNo++];
//                        b.byteBuffer.flip();
//                        b.byteBuffer.limit(b.bufferIndex*7);
//                        try {
//                            fw.fileChannel.write(b.byteBuffer);
//                            b.bufferIndex = 0;
//                            b.byteBuffer.clear();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    } while (flushNo < PARTITION);
//                });
//
//                fs[1] = executorService.submit(() -> {
//                    int flushNo = 0;
//                    do {
//                        BucketFile b = flushPartArray[flushNo];
//                        FileWriter fw = partQueryEngine.fileWriters[flushNo++];
//                        b.byteBuffer.flip();
//                        b.byteBuffer.limit(b.bufferIndex*7);
//                        try {
//                            fw.fileChannel.write(b.byteBuffer);
//                            b.bufferIndex = 0;
//                            b.byteBuffer.clear();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    } while (flushNo < PARTITION);
//                });
//
//                for (Future f : fs) {
//                    f.get();
//                }


//                long metaStart = System.currentTimeMillis();
//        fs = new Future[2];
//        fs[0] = executorService.submit(()-> {
//            try {
//                RandomAccessFile fw = new RandomAccessFile(orderMeta, "rw");
//                ByteBuffer b = ByteBuffer.allocateDirect(orderKeyPosition[0].length * 8);
////                        long adds = ((DirectBuffer) b).address();
//                for (int i = 0; i < orderKeyPosition[0].length; i++) {
////                    long s = orderQueryEngine.fileWriters[i].fileChannel.size();
////                    orderKeyPosition[orderIdx++] = s;
//                    b.putLong(orderKeyPosition[0][i] + orderKeyPosition[1][i] + orderKeyPosition[2][i]);
////                            unsafe.putLong(adds + i * 8L, s);
//                }
//                b.flip();
//                fw.getChannel().write(b);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//
//        fs[1] = executorService.submit(()-> {
//            try {
//                RandomAccessFile fw = new RandomAccessFile(partMeta, "rw");
//                ByteBuffer b = ByteBuffer.allocateDirect(orderKeyPosition[0].length * 8);
////                        long adds = ((DirectBuffer) b).address();
//                for (int i = 0; i < partKeyPosition[0].length; i++) {
////                    long s = partQueryEngine.fileWriters[i].fileChannel.size();
////                    partKeyPosition[partIdx++] = s;
//                    b.putLong(partKeyPosition[0][i]+partKeyPosition[1][i]+partKeyPosition[2][i]);
////                            unsafe.putLong(adds + i * 8L, s);
//                }
//                b.flip();
//                fw.getChannel().write(b);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//
//        for (Future f : fs) {
//            f.get();
//        }
//
//        long sum = 0;
//        long sum2 = 0;
//        for (int i = 0; i < orderKeyPosition[0].length; i++) {
//            sum += orderKeyPosition[0][i] + orderKeyPosition[1][i] + orderKeyPosition[2][i];
//            sum2 += partKeyPosition[0][i]+partKeyPosition[1][i]+partKeyPosition[2][i];
//            System.out.println(i + " " + sum + " " + sum2);
//        }

//                System.out.println("write to meta cost" + (System.currentTimeMillis() - metaStart));

        //                for (int j = 0; j < 512; j++) {
        //                    long dataNum = 0;
        //                    for (int i = 0; i < threadNum; i++) {
        //                        dataNum += orderRaceEngine.bucketFiles[i][j].getDataNum();
        //                    }
        //                    System.out.println("partition " + j + " has " + dataNum + " nums");
        //                }

//                long s = 0;
//                long f = 0;
//                for (int i = 0; i < PARTITION; i++) {
//                    f += partKeyPosition[i];
//                    s += partKeyPosition[PARTITION + i] - partKeyPosition[i];
//                    System.out.println("endPosition:" + partKeyPosition[PARTITION + i] + " startPosition" + partKeyPosition[i] + " totalSize:" + s + " first totalSize:" + f);
//                }

        System.out.println("read + analysis cost " + (System.currentTimeMillis() - start) + " ms");

        //                for (Map.Entry kv : columnName2EngineMap.entrySet()) {
        //                    DiskQueryEngine eee = (DiskQueryEngine) kv.getValue();
        //                    long nums = 0;
        //                    for (int j = 0; j < PARTITION; j++) {
        ////                        System.out.println(kv.getKey() + " | thread 0  partition " + j + " nums " +
        ////                                eee.fileWriters[j].getDataNum());
        //                        nums += eee.fileWriters[j].getDataNum();
        //                    }
        //                    System.out.println(kv.getKey() + " | " + nums);
        //                }

        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_0");
        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_1");
        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_0_2");
        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_0");
        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_1");
        //                testUnsafeResult(workspaceDir+File.separator+"lineitem.l_orderkey_1_2");

        System.out.println("total cost " + (System.currentTimeMillis() - sss) + " ms");


        //        File workDir = new File(workspaceDir);
        //        for (File dataFile : Objects.requireNonNull(workDir.listFiles())) {
        //            System.out.println(dataFile.getName() + " " + dataFile.length());
        //        }
        //        throw new RuntimeException("test");
    }

    //    private ConcurrentHashMap<Long, Boolean> a = new ConcurrentHashMap<Long, Boolean>();
    private AtomicInteger acc = new AtomicInteger(1);
    private AtomicLong costTime = new AtomicLong(1);

    public void testUnsafeResult(String path) throws IOException {
        File testWorkspaceDir = new File(path);
        RandomAccessFile rfile = new RandomAccessFile(testWorkspaceDir, "r");
        System.out.println(rfile.length());
        FileChannel rfc = rfile.getChannel();
        ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
        rfc.read(buf2);
        long adad = ((DirectBuffer) buf2).address();
        System.out.println(unsafe.getLong(adad));
        System.out.println(unsafe.getLong(adad+8));
        System.out.println(unsafe.getLong(adad+8+8));
        System.out.println(unsafe.getLong(adad+8+8+8));
        System.out.println(unsafe.getLong(adad+8+8+8+8));
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        //        if (acc.getAndIncrement() <= 2) {
        //            new Thread(VmstatLogger::useLinuxCommond2).start();
        //        }
        int t = acc.getAndIncrement();
        if (t == 1) {
            System.out.println(System.currentTimeMillis());
            costTime.set(System.currentTimeMillis());
        }
        if (acc.compareAndSet(3995, 0)) {
            throw new RuntimeException("test cost: " + (System.currentTimeMillis() - costTime.get()));
        }
        //        if (!a.containsKey(Thread.currentThread().getId())) {
        //            a.put(Thread.currentThread().getId(), true);
        //            System.out.println(a.size());
        //            if (a.size() > 8) {
        //                throw new RuntimeException("gt 8");
        //            } else if (acc.compareAndSet(200, 0)) {
        //                throw new RuntimeException("test");
        //            }
        //        }

        //        System.out.println("=============");
        //        for (Map.Entry kv : columnName2EngineMap.entrySet()) {
        //            DiskRaceEngine partRaceEngine = (DiskRaceEngine) kv.getValue();
        //            int a = 0;
        //            for (int i = 0; i < WRITE_THREAD_NUM; i++) {
        //                for (int j = 0; j < PARTITION; j++) {
        ////                    System.out.println(kv.getKey() + " | thread " + i + " partition " + j + " nums " +
        ////                            partRaceEngine.bucketFiles[i][j].getDataNum());
        //                    a += partRaceEngine.bucketFiles[i][j].getDataNum();
        //                }
        //            }
        //            System.out.println(a);
        //        }

//        long start = System.currentTimeMillis();
        DiskQueryEngine raceEngine = columnName2EngineMap.get(tableColumnKey(column));
        long[] position = tableColumnPosition(column);
        long ans;
        if (table.equals("lineitem")) {
            ans = raceEngine.quantile(percentile, 0, position);
        } else {
            ans = raceEngine.quantile(percentile, 1, position);
        }
//        long cost = System.currentTimeMillis() - start;
        //        costTime.addAndGet((int) cost);
//        System.out.println(
//                "Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans + ", Cost " + cost + " ms");
        return ans + "";
    }

    //    @Override
    //    public String quantile(String table, String column, double percentile) throws Exception {
    //        long start = System.currentTimeMillis();
    //        System.out.println(table + " " + column);
    //        DiskRaceEngine diskRaceEngine = columnName2EngineMap.get(tableColumnKey(table, column));
    //        System.out.println(diskRaceEngine == null);
    //        long ans = diskRaceEngine.quantile(percentile);
    //        long cost = System.currentTimeMillis() - start;
    //        System.out.println(
    //            "Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans + ", Cost " + cost + " ms");
    //        //if (atomicInteger.incrementAndGet() == 9) {
    //        //    return "12345";
    //        //}
    //        return ans + "";
    //    }

    private String tableColumnKey(String column) {
        if (column.equals("L_ORDERKEY") || column.equals("O_ORDERKEY")) {
            return "ORDERKEY";
        }

        return "PARTKEY";
//        return (table + "." + column).toLowerCase();
    }

    private long[] tableColumnPosition(String column) {
        if (column.equals("L_ORDERKEY") || column.equals("O_ORDERKEY")) {
            return orderPosition;
        }

        return partPosition;
    }

    public void initPosition(String workspaceDir) throws Exception {
        Future[] fs = new Future[6];
        fs[4] = executorService.submit(() -> {
            try {
                RandomAccessFile r = new RandomAccessFile(orderMeta, "rw");
                FileChannel fc = r.getChannel();
                ByteBuffer bf = ByteBuffer.allocate(PARTITION * 2 * 8);
                bf.clear();
                fc.read(bf);
                bf.flip();
                int idx = 0;
                while (bf.hasRemaining()) {
                    orderPosition[idx++] = bf.getLong();
                }
                bf.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        fs[5] = executorService.submit(() -> {
            try {
                RandomAccessFile r = new RandomAccessFile(partMeta, "rw");
                FileChannel fc = r.getChannel();
                ByteBuffer bf = ByteBuffer.allocate(PARTITION * 2 * 8);
                bf.clear();
                fc.read(bf);
                bf.flip();
                int idx = 0;
                while (bf.hasRemaining()) {
                    partPosition[idx++] = bf.getLong();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        final String col = "ORDERKEY";
        orderQueryEngine = new DiskQueryEngine(workspaceDir, col);
        columnName2EngineMap.put(col, orderQueryEngine);
        fs[0] = executorService.submit(() -> {
//                        orderQueryEngine.init(0);
            for (int j = 0; j < 1024; j++) {
                orderQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col + "_0_" + j);
            }
        });
        fs[1] = executorService.submit(() -> {
//                        orderQueryEngine.init(0);
            for (int j = 1024; j < PARTITION; j++) {
                orderQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col + "_0_" + j);
            }
        });

        final String col2 = "PARTKEY";
        partQueryEngine = new DiskQueryEngine(workspaceDir, col2);
        columnName2EngineMap.put(col2, partQueryEngine);
        fs[2] = executorService.submit(() -> {
//                        partQueryEngine.init(0);
            for (int j = 0; j < 1024; j++) {
                partQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col2 + "_0_" + j);
            }
        });

        fs[3] = executorService.submit(() -> {
//                        partQueryEngine.init(0);
            for (int j = 1024; j < PARTITION; j++) {
                partQueryEngine.fileWriters[j] = new FileWriter(workspaceDir + File.separator + col2 + "_0_" + j);
            }
        });

        for (Future f : fs) {
            f.get();
        }
    }
}