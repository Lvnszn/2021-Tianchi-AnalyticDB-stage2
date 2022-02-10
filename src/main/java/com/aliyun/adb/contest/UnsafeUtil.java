package com.aliyun.adb.contest;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeUtil {

    public static final Unsafe unsafe = createUnsafe();
    public static long LONG_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
    public static long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    private static Unsafe createUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Can't use unsafe", e);
        }
    }

    public static long getMemoryAddress(ByteBuffer buf) throws UnsupportedOperationException {
        long address;
        try {
            Field addressField = java.nio.Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            address = addressField.getLong(buf);
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
        return address;
    }
}