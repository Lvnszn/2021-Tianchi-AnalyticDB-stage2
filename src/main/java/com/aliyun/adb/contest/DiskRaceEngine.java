package com.aliyun.adb.contest;

import static com.aliyun.adb.contest.RaceAnalyticDB.*;


public class DiskRaceEngine {
    public BucketFile[][] bucketFiles;

    public DiskRaceEngine() {
        bucketFiles = new BucketFile[2][PARTITION];
    }
}