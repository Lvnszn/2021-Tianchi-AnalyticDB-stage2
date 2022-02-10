package com.aliyun.adb.contest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentController {

    public CyclicBarrier readBarrier = new CyclicBarrier(4);
    public CountDownLatch sourceDownLatch = new CountDownLatch(4);
    public CountDownLatch readDownLatch = new CountDownLatch(6);
    public CountDownLatch countDownLatch = new CountDownLatch(4);
    public CountDownLatch dispatchLatch = new CountDownLatch(2);


}
