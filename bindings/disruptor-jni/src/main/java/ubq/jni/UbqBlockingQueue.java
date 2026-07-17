package ubq.jni;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link BlockingQueue} adapter for LMAX Disruptor queue-baseline experiments.
 *
 * <p>UBQ is unbounded and non-blocking. Enqueue methods never block; dequeue
 * methods that need blocking semantics spin until an item is available or a
 * timeout expires. The primitive {@link UbqLongQueue} API is preferred for
 * throughput measurements that can be modified to avoid boxing.</p>
 */
public final class UbqBlockingQueue extends AbstractQueue<Long>
        implements BlockingQueue<Long>, AutoCloseable {
    private static final long EMPTY = Long.MIN_VALUE;

    private final UbqLongQueue queue;
    private final AtomicInteger size = new AtomicInteger();

    public UbqBlockingQueue() {
        this.queue = new UbqLongQueue();
    }

    @Override
    public boolean offer(Long value) {
        Objects.requireNonNull(value, "value");
        if (value == EMPTY) {
            throw new IllegalArgumentException("Long.MIN_VALUE is reserved as the empty sentinel");
        }
        queue.push(value.longValue());
        size.incrementAndGet();
        return true;
    }

    @Override
    public void put(Long value) {
        offer(value);
    }

    @Override
    public boolean offer(Long value, long timeout, TimeUnit unit) {
        offer(value);
        return true;
    }

    @Override
    public Long poll() {
        long value = queue.popOr(EMPTY);
        if (value == EMPTY) {
            return null;
        }
        size.decrementAndGet();
        return Long.valueOf(value);
    }

    @Override
    public Long take() throws InterruptedException {
        Long value;
        while ((value = poll()) == null) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            Thread.onSpinWait();
        }
        return value;
    }

    @Override
    public Long poll(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        Long value;
        while ((value = poll()) == null) {
            if (System.nanoTime() - deadline >= 0) {
                return null;
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            Thread.onSpinWait();
        }
        return value;
    }

    @Override
    public Long peek() {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super Long> target) {
        return drainTo(target, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super Long> target, int maxElements) {
        Objects.requireNonNull(target, "target");
        if (target == this) {
            throw new IllegalArgumentException("cannot drain to self");
        }
        if (maxElements <= 0) {
            return 0;
        }

        int drained = 0;
        Long value;
        while (drained < maxElements && (value = poll()) != null) {
            target.add(value);
            drained++;
        }
        return drained;
    }

    @Override
    public Iterator<Long> iterator() {
        throw new UnsupportedOperationException("UBQ does not support snapshots");
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public void close() {
        queue.close();
    }
}
