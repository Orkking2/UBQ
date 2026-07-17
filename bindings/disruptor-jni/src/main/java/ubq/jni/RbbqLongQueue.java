package ubq.jni;

import java.lang.ref.Cleaner;

/**
 * Primitive JNI wrapper around the rbbq FastFifo backend used by UBQ's Rust
 * benchmark harness.
 */
public final class RbbqLongQueue implements AutoCloseable {
    public static final int DEFAULT_BLOCK_SIZE = Integer.getInteger("ubq.jni.rbbq.blockSize", 64);

    private static final Cleaner CLEANER = Cleaner.create();

    static {
        String explicitLibrary = System.getProperty("ubq.jni.library");
        if (explicitLibrary == null || explicitLibrary.isBlank()) {
            System.loadLibrary("ubq");
        } else {
            System.load(explicitLibrary);
        }
    }

    private final NativeState state;
    private final Cleaner.Cleanable cleanable;

    public RbbqLongQueue(long capacity) {
        this(capacity, DEFAULT_BLOCK_SIZE);
    }

    public RbbqLongQueue(long capacity, int blockSize) {
        requirePositive(capacity, "capacity");
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }

        long handle = nativeCreate(capacity, blockSize);
        if (handle == 0) {
            throw new IllegalStateException("failed to create native RBBQ");
        }
        this.state = new NativeState(handle);
        this.cleanable = CLEANER.register(this, state);
    }

    public void pushConstantExact(long value, long count) {
        requireNonNegative(count, "count");
        nativePushConstantExact(handle(), value, count);
    }

    public void pushRangeExact(long firstValue, long count) {
        requireNonNegative(count, "count");
        nativePushRangeExact(handle(), firstValue, count);
    }

    public static void pushRangeToThreeExact(
            RbbqLongQueue first,
            RbbqLongQueue second,
            RbbqLongQueue third,
            long firstValue,
            long count) {
        requireNonNegative(count, "count");
        nativePushRangeToThreeExact(first.handle(), second.handle(), third.handle(), firstValue, count);
    }

    public long popAddExact(long count) {
        requireNonNegative(count, "count");
        return nativePopAddExact(handle(), count);
    }

    public long popSubExact(long count) {
        requireNonNegative(count, "count");
        return nativePopSubExact(handle(), count);
    }

    public long popAndExact(long count) {
        requireNonNegative(count, "count");
        return nativePopAndExact(handle(), count);
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private long handle() {
        long handle = state.handle;
        if (handle == 0) {
            throw new IllegalStateException("native RBBQ is closed");
        }
        return handle;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static final class NativeState implements Runnable {
        private volatile long handle;

        private NativeState(long handle) {
            this.handle = handle;
        }

        @Override
        public synchronized void run() {
            long current = handle;
            if (current != 0) {
                handle = 0;
                nativeDestroy(current);
            }
        }
    }

    private static native long nativeCreate(long capacity, int blockSize);

    private static native void nativeDestroy(long handle);

    private static native void nativePushConstantExact(long handle, long value, long count);

    private static native void nativePushRangeExact(long handle, long firstValue, long count);

    private static native void nativePushRangeToThreeExact(
            long firstHandle, long secondHandle, long thirdHandle, long firstValue, long count);

    private static native long nativePopAddExact(long handle, long count);

    private static native long nativePopSubExact(long handle, long count);

    private static native long nativePopAndExact(long handle, long count);
}
