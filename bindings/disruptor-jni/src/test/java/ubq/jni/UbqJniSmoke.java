package ubq.jni;

public final class UbqJniSmoke {
    private UbqJniSmoke() {
    }

    public static void main(String[] args) {
        for (String variant : UbqLongQueue.NATIVE_VARIANTS) {
            try (UbqLongQueue queue = new UbqLongQueue(variant)) {
                if (!variant.equals(queue.variant())) {
                    throw new AssertionError("unexpected variant label: " + queue.variant());
                }
                queue.push(11);
                queue.push(12);
                if (queue.popAddExact(2) != 23) {
                    throw new AssertionError("native variant smoke failed for " + variant);
                }
            }
        }

        try (UbqLongQueue queue = new UbqLongQueue()) {
            queue.push(41);
            queue.pushRange(42, 4);

            long first = queue.popOr(-1);
            int batch = queue.popBatch(3);
            long last = queue.popOr(-1);
            long empty = queue.popOr(-1);

            if (first != 41 || batch != 3 || last != 45 || empty != -1) {
                throw new AssertionError(
                        "unexpected UBQ JNI smoke result: first=" + first
                                + " batch=" + batch
                                + " last=" + last
                                + " empty=" + empty);
            }
        }

        try (UbqLongQueue queue = new UbqLongQueue()) {
            queue.pushConstantExact(3, 4);
            if (queue.popAddExact(4) != 12) {
                throw new AssertionError("native exact add failed");
            }

            queue.pushRangeExact(1, 4);
            if (queue.popSubExact(4) != -10) {
                throw new AssertionError("native exact subtract failed");
            }

            queue.pushRangeExact(1, 4);
            if (queue.popAndExact(4) != 0) {
                throw new AssertionError("native exact and failed");
            }
        }

        try (
                UbqLongQueue first = new UbqLongQueue();
                UbqLongQueue second = new UbqLongQueue();
                UbqLongQueue third = new UbqLongQueue()) {
            UbqLongQueue.pushRangeToThreeExact(first, second, third, 1, 3);
            if (first.popAddExact(3) != 6 || second.popSubExact(3) != -6 || third.popAndExact(3) != 0) {
                throw new AssertionError("native exact three-queue range failed");
            }
        }

        try (UbqBlockingQueue queue = new UbqBlockingQueue()) {
            queue.offer(7L);
            if (!Long.valueOf(7L).equals(queue.poll())) {
                throw new AssertionError("blocking adapter did not return queued value");
            }
        }
    }
}
