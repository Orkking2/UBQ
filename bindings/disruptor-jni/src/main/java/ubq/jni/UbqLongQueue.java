package ubq.jni;

import java.lang.ref.Cleaner;

/**
 * Primitive JNI wrapper around UBQ for throughput benchmarks.
 *
 * <p>The native queue stores Java {@code long} values bit-for-bit in a UBQ
 * {@code u64}. This class intentionally exposes batch methods that do all
 * per-item queue operations on the native side, so benchmark results are not
 * dominated by one JNI transition per event.</p>
 */
public final class UbqLongQueue implements AutoCloseable {
    public static final String DEFAULT_NATIVE_VARIANT = "balanced,8,127,crossbeam";

    /** @deprecated use {@link #DEFAULT_NATIVE_VARIANT}. */
    @Deprecated
    public static final String NATIVE_VARIANT = DEFAULT_NATIVE_VARIANT;

    public static final String[] NATIVE_VARIANTS = {
            "balanced,0,127,crossbeam",
            "balanced,4,127,crossbeam",
            "balanced,8,63,crossbeam",
            "balanced,8,127,crossbeam",
            "balanced,8,255,crossbeam",
            "balanced,16,127,crossbeam",
            "balanced,32,127,crossbeam",
            "balanced,8,31,crossbeam",
            "balanced,8,511,crossbeam",
            "balanced,8,127,yield",
    };

    private static final Cleaner CLEANER = Cleaner.create();

    static {
        String explicitLibrary = System.getProperty("ubq.jni.library");
        if (explicitLibrary == null || explicitLibrary.isBlank()) {
            System.loadLibrary("ubq");
        } else {
            System.load(explicitLibrary);
        }

        if (NATIVE_VARIANTS.length != nativeVariantCount()) {
            throw new UnsatisfiedLinkError(
                    "UBQ JNI variant registry mismatch: Java has " + NATIVE_VARIANTS.length
                            + " variants, native library has " + nativeVariantCount());
        }
        int defaultVariantId = nativeDefaultVariantId();
        if (defaultVariantId < 0
                || defaultVariantId >= NATIVE_VARIANTS.length
                || !DEFAULT_NATIVE_VARIANT.equals(NATIVE_VARIANTS[defaultVariantId])) {
            throw new UnsatisfiedLinkError("UBQ JNI default variant mismatch");
        }
    }

    private final NativeState state;
    private final Cleaner.Cleanable cleanable;
    private final String variant;

    public UbqLongQueue() {
        this(System.getProperty("ubq.jni.ubqVariant", DEFAULT_NATIVE_VARIANT));
    }

    public UbqLongQueue(String variant) {
        int variantId = variantId(variant);
        long handle = nativeCreateVariant(variantId);
        if (handle == 0) {
            throw new IllegalStateException("failed to create native UBQ variant " + variant);
        }
        this.state = new NativeState(handle);
        this.cleanable = CLEANER.register(this, state);
        this.variant = NATIVE_VARIANTS[variantId];
    }

    public String variant() {
        return variant;
    }

    public void push(long value) {
        nativePush(handle(), value);
    }

    public void pushRange(long firstValue, int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }
        nativePushRange(handle(), firstValue, count);
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
            UbqLongQueue first,
            UbqLongQueue second,
            UbqLongQueue third,
            long firstValue,
            long count) {
        requireNonNegative(count, "count");
        nativePushRangeToThreeExact(first.handle(), second.handle(), third.handle(), firstValue, count);
    }

    public long popOr(long emptyValue) {
        return nativePopOr(handle(), emptyValue);
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

    public int popBatch(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        return nativePopBatch(handle(), limit);
    }

    public boolean isEmpty() {
        return nativeIsEmpty(handle());
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private long handle() {
        long handle = state.handle;
        if (handle == 0) {
            throw new IllegalStateException("native UBQ is closed");
        }
        return handle;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static int variantId(String variant) {
        if (variant == null || variant.isBlank()) {
            variant = DEFAULT_NATIVE_VARIANT;
        }
        String normalized = variant.trim();
        for (int i = 0; i < NATIVE_VARIANTS.length; i++) {
            if (NATIVE_VARIANTS[i].equals(normalized)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "unsupported UBQ JNI variant '" + variant + "'; supported variants: "
                        + String.join("; ", NATIVE_VARIANTS));
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

    private static native long nativeCreate();

    private static native long nativeCreateVariant(int variantId);

    private static native int nativeVariantCount();

    private static native int nativeDefaultVariantId();

    private static native void nativeDestroy(long handle);

    private static native void nativePush(long handle, long value);

    private static native void nativePushRange(long handle, long firstValue, int count);

    private static native void nativePushConstantExact(long handle, long value, long count);

    private static native void nativePushRangeExact(long handle, long firstValue, long count);

    private static native void nativePushRangeToThreeExact(
            long firstHandle, long secondHandle, long thirdHandle, long firstValue, long count);

    private static native long nativePopOr(long handle, long emptyValue);

    private static native long nativePopAddExact(long handle, long count);

    private static native long nativePopSubExact(long handle, long count);

    private static native long nativePopAndExact(long handle, long count);

    private static native int nativePopBatch(long handle, int limit);

    private static native boolean nativeIsEmpty(long handle);
}
