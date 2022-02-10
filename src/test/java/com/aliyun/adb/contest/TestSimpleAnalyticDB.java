package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import org.junit.Assert;
import org.junit.Test;
import java.nio.ByteOrder;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.aliyun.adb.contest.RaceAnalyticDB.WRITE_BUFFER_SIZE;
import static com.aliyun.adb.contest.UnsafeUtil.getMemoryAddress;
import static com.aliyun.adb.contest.UnsafeUtil.unsafe;

public class TestSimpleAnalyticDB {

    private static final Unsafe unsafe = createUnsafe();
    long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);

    ConcurrentLinkedQueue<TestBucket> aaaa = new ConcurrentLinkedQueue<>();
    private static Unsafe createUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Can't use unsafe", e);
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    private static int dataSize = 4 + 8 + 1 + 8 + 4 + 2 + 3 + 2*2 + 3*4 + 3*8 + 3*4 + 3*8;


    @Test
    public void testBucketCompact() throws IOException {
        TestBucket[] t = new TestBucket[10];
        System.out.println(t[0].Name);
    }

    @Test
    public void testCompact() throws IOException {
        long a = 1231231231L;
        long b = 3213255555555555211L;

        ByteBuffer bb = ByteBuffer.allocateDirect(13);
        long add = ((DirectBuffer) bb).address();
        unsafe.putLong(add, (a << 12 | b >> 52));
        unsafe.putInt(add + 8, (int) (b >> 8 & 0xFF));
        unsafe.putByte(add + 12, (byte) (b & 0xF));
        System.out.println(unsafe.getLong(add));
        System.out.println((byte)',');
        System.out.println((byte)'\n');
//        unsafe.putInt();
//        unsafe.putByte();
    }
    @Test
    public void testOr() throws IOException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/test2");
        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(16);
//        buf.clear();
//        putData(buf);
        long adress = ((DirectBuffer) buf).address();
        unsafe.putLong(adress, 9196065309794938857L);
        unsafe.putLong(adress+7, 1003L);

        //-23
        //-81
        //-98
        //41
        //-83
        //-4
        //-98
        //127

        buf.limit(15);
        fc.write(buf);
//        unsafe.putLong(adress, 1002L<<8);
//        unsafe.putLong(adress+8, 1001L);
//        buf.limit(7);
//        fc.write(buf, 14);

//        System.out.println(9196065309794938857L >> 54); // 510
//        System.out.println(9196065309794938857L << 8);
//        System.out.println(9196065309794938857L << 8 >>> 8);
//        System.out.println((9196065309794938857L << 8 >>> 8) + (510L << 54));

        RandomAccessFile rfile = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel rfc = rfile.getChannel();
        ByteBuffer buf2 = ByteBuffer.allocateDirect(14);
        rfc.position(0);
        rfc.read(buf2);
        long adad = ((DirectBuffer) buf2).address();
        byte[] longBytes = new byte[8];
        unsafe.copyMemory(null, adad, longBytes, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, 8);

        System.out.println(longBytes[0]);
        longBytes[7] = (byte) ((510L >> 2) & 0xFF);
        System.out.println(unsafe.getLong(longBytes, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET));
        System.out.println(bytesToLong(longBytes));
        System.out.println(510>>2 & 0xFF);


        System.out.println((unsafe.getLong(adad)>>>7)|(510L<<54));
//        System.out.println(longVal);
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.nativeOrder());
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    @Test
    public void testPrintMaxValue() {
        System.out.println(Long.MAX_VALUE * 0.06784);
        System.out.println(Long.MAX_VALUE * 0.06785);
        System.out.println(625800000000000000L + 5.79270059648E12);
    }

    @Test
    public void testModify() {
        TestBucket[] tb = new TestBucket[1];
        TestBucket realBucket = new TestBucket();
        realBucket.Name = "Hello Jason";
        tb[0] = realBucket;

        aaaa.add(tb[0]);
        TestBucket tbModify = aaaa.poll();
        tbModify.Name = "Goodbye Jason";

        System.out.println(realBucket.Name);
        System.out.println(tb[0].Name);
        System.out.println(tbModify.Name);
    }
    
    @Test
    public void testUnsafeCopy() {
        long[] o = new long[10];
        o[0] = 100;
        long[] d = new long[2];
        d[1] = 111;
        System.out.println(o[0]);
        unsafe.copyMemory(o, BYTE_ARRAY_BASE_OFFSET, d, BYTE_ARRAY_BASE_OFFSET, 8);
        System.out.println(d[0]);
        System.out.println(d[1]);
    }

    @Test
    public void testUnsafeWrite() throws IOException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/test");
        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(155);
//        buf.clear();
//        putData(buf);
        long adress = ((DirectBuffer) buf).address();
        unsafe.putLong(adress, 1004L);
        unsafe.putLong(adress+8, 1003L);
        buf.limit(16);
        fc.write(buf);
        unsafe.putLong(adress, 1002L);
//        unsafe.putLong(adress+8, 1001L);
        buf.limit(16);
        fc.write(buf, 16);


        RandomAccessFile rfile = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel rfc = rfile.getChannel();
        System.out.println(rfc.size());
        ByteBuffer buf2 = ByteBuffer.allocateDirect(32);
        rfc.position(0);
        rfc.read(buf2);
        long adad = ((DirectBuffer) buf2).address()+8;
        System.out.println(unsafe.getLong(adad));
    }


    @Test
    public void testByteWrite() throws IOException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/test1");
        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        buf.clear();
//        putData(buf);
        buf.putLong(1004L);
        buf.putLong(1003L);
        buf.flip();
        fc.write(buf);


        RandomAccessFile rfile = new RandomAccessFile(testWorkspaceDir, "rw");
        FileChannel rfc = rfile.getChannel();
        System.out.println(rfc.size());
        ByteBuffer buf2 = ByteBuffer.allocateDirect(32);
        rfc.position(0);
        rfc.read(buf2);
        buf2.flip();
        System.out.println(buf2.getLong());
        System.out.println(buf2.getLong());
        System.out.println(buf2.getLong());
    }

    private static void putData(ByteBuffer buf) {
        buf.order(ByteOrder.nativeOrder());
        buf.putInt(655350);
        buf.putLong(10034534500L);
        buf.put((byte) 1);
        buf.putDouble(1.5);
        buf.putFloat((float) 2.5);
        buf.putShort((short) 2);
        buf.put(new byte[] {(byte) 15, (byte) 25, (byte) 35});
        buf.putShort((short) 10);
        buf.putShort((short) 1000);
        buf.putInt(655360);
        buf.putInt(23);
        buf.putInt(134);
        buf.putLong(134134234L);
        buf.putLong(-23454352346L);
        buf.putLong(3245245425L);
        buf.putFloat((float) 1.0);
        buf.putFloat((float) 2.0);
        buf.putFloat((float) 4.0);
        buf.putDouble(1.1);
        buf.putDouble(2.2);
        buf.putDouble(3.3);
//        buf.rewind();
    }

    @Test
    public void testUnsafe() throws IOException, NoSuchFieldException, IllegalAccessException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/test");
//        testWorkspaceDir.createNewFile();
        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "r");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(2048);
        fc.read(buf);
        buf.clear();

        long adress = ((DirectBuffer) buf).address();
        System.out.println(unsafe.getInt(adress));
        System.out.println(unsafe.getLong(adress+8));
        for (int i = 0; i < 100; i++) {
            System.out.println(unsafe.getLong(null, getMemoryAddress(buf)));
        }
//        2522449770
//        int i = 0;
//        while (i < 2048/8) {
//            long v = unsafe.getLong(adress);
//            adress += 8;
//            i += 1;
//            System.out.println(v);
//        }
    }
    volatile int a = 0;
    @Test
    public void testConcurrent() throws InterruptedException {
        ArrayBlockingQueue<long[]> req = new ArrayBlockingQueue<>(1000);

        CountDownLatch latch = new CountDownLatch(3);
        CountDownLatch readLatch = new CountDownLatch(2);
        for (int i = 0; i < 3; i++) {
            new Thread(()->{
                long[] test = new long[100];
                for (int j = 0; j < 10000; j++) {
                    if (j%100 == 0) {
                        try {
                            req.put(test);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    test[j%100] = 1000;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                latch.countDown();
            }).start();
        }

        for (int i =0; i < 2; i++) {
            new Thread(()-> {
                int a = 0;
                try {
                    while(true) {
                        long[] r = req.poll(100, TimeUnit.MILLISECONDS);
                        if (r == null ) {
                            break;
                        }
                        a += r.length;
                        System.out.println(a);
                    }
                    System.out.println("test");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                readLatch.countDown();
            }).start();
        }

        latch.await();
        System.out.println("six six six");
        readLatch.await();
    }

    @Test
    public void testOut() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 4000*3; i++) {
            ByteBuffer bf = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
//            long add = ((DirectBuffer) bf).address();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testReadPartition() throws IOException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/lineitem.l_orderkey_0_20");

        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "r");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect((int) fc.size());
        fc.read(buf);
        buf.flip();
        for (int i = 0; i < fc.size()/8; i++) {
            long adad = ((DirectBuffer) buf).address();
            System.out.println(unsafe.getLong(adad + i*8));
        }
    }

    @Test
    public void testPartition() throws IOException {
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target/lineitem.l_orderkey_0_1");

        RandomAccessFile file = new RandomAccessFile(testWorkspaceDir, "r");
        FileChannel fc = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocateDirect(2048);
        fc.read(buf);
        buf.flip();
        while(buf.hasRemaining()) {
            long values = buf.getLong();
            int a = (int)(values >> 56);
            System.out.println(values);
        }
    }

    @Test
    public void testCorrectness() throws Exception {
        File testDataDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/test_data");
        File testWorkspaceDir = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/target");
        File testResultsFile = new File("/Users/jason/IdeaProjects/2021-tianchi-contest-2/test_result/results");
        List<String> ans = new ArrayList<>();

        try (BufferedReader resReader = new BufferedReader(new FileReader(testResultsFile))) {
            String line;
            while ((line = resReader.readLine()) != null) {
                ans.add(line);
            }
        }

        // ROUND #1
        RaceAnalyticDB analyticDB1 = new RaceAnalyticDB();
        analyticDB1.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());
        testQuery(analyticDB1, ans, 10);

        // To simulate exiting
//        analyticDB1 = null;

        // ROUND #2
        RaceAnalyticDB analyticDB2 = new RaceAnalyticDB();
        analyticDB2.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());

        Executor testWorkers = Executors.newFixedThreadPool(8);

        CompletableFuture[] futures = new CompletableFuture[8];

        for (int i = 0; i < 8; i++) {
            futures[i] = CompletableFuture.runAsync(() -> testQuery(analyticDB2, ans, 500), testWorkers);
        }

        CompletableFuture.allOf(futures).get();
    }

    private void testQuery(AnalyticDB analyticDB, List<String> ans, int testCount) {
        try {
            for (int i = 0; i < testCount; i++) {
                int p = ThreadLocalRandom.current().nextInt(ans.size());
                String resultStr[] = ans.get(p).split(" ");
                String table = resultStr[0];
                String column = resultStr[1];
                double percentile = Double.valueOf(resultStr[2]);
                String answer = resultStr[3];

                Assert.assertEquals(answer, analyticDB.quantile(table, column, percentile));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
