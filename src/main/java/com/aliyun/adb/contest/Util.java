package com.aliyun.adb.contest;

import static com.aliyun.adb.contest.RaceAnalyticDB.PARTITION_OVER_PARTITION;
import static com.aliyun.adb.contest.RaceAnalyticDB.THREAD_NUM;


public class Util {

    //public static long[] sharedBuffer = new long[498_3700];
    //public static long[] sharedBuffer = new long[260_0000];
    //public static long[] sharedBuffer = new long[260_0000];

//    public static long[] sharedBuffer = new long[600_0000];
//    public static long[][][] sharedBuffers = new long[THREAD_NUM][PARTITION_OVER_PARTITION][50_0000];
//    public static int[][] sharedIndex = new int[THREAD_NUM][PARTITION_OVER_PARTITION];

    public static long quickSelect(long[] nums, int start, int end, int k) {
        if (start == end) {
            return nums[start];
        }
        int left = start;
        int right = end;
        long pivot = nums[(start + end) / 2];
        while (left <= right) {
            while (left <= right && nums[left] > pivot) {
                left++;
            }
            while (left <= right && nums[right] < pivot) {
                right--;
            }
            if (left <= right) {
                long temp = nums[left];
                nums[left] = nums[right];
                nums[right] = temp;
                left++;
                right--;
            }
        }
        if (start + k - 1 <= right) {
            return quickSelect(nums, start, right, k);
        }
        if (start + k - 1 >= left) {
            return quickSelect(nums, left, end, k - (left - start));
        }
        return nums[right + 1];
    }

    public static int quickSelectV2(long[] nums, int start, int end, int k) {
        long pivot;
        int left, right;

        while (true) {
            int length = end - start + 1;
            if (length > 1000) {
                int sampleNums = length / 100;

                double p = (k - start - 1.0) / length;

                if (p < 0.4) {
                    p += 0.05;
                } else if (p > 0.6) {
                    p -= 0.05;
                }

                int sK = (int)(Math.max(sampleNums * p, 1) + start);

                left = quickSelectV2(nums, start, start + sampleNums, sK);
                pivot = nums[left];
            } else {
                pivot = nums[start];
                left = start;
            }

            right = end;
            while (left < right) {
                while (nums[right] > pivot) right--;
                nums[left] = nums[right];

                while (left < right && nums[left] <= pivot) left++;
                nums[right] = nums[left];
            }

            nums[left] = pivot;

            if (left < k - 1) {
                start = left + 1;
            } else if (left > k - 1) {
                end = left - 1;
            } else {
                return left;
            }
        }
    }
}