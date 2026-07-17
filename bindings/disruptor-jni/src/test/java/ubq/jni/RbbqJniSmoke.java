package ubq.jni;

public final class RbbqJniSmoke {
    private RbbqJniSmoke() {
    }

    public static void main(String[] args) {
        try (RbbqLongQueue queue = new RbbqLongQueue(64, 8)) {
            queue.pushConstantExact(3, 4);
            if (queue.popAddExact(4) != 12) {
                throw new AssertionError("RBBQ native exact add failed");
            }

            queue.pushRangeExact(1, 4);
            if (queue.popSubExact(4) != -10) {
                throw new AssertionError("RBBQ native exact subtract failed");
            }

            queue.pushRangeExact(1, 4);
            if (queue.popAndExact(4) != 0) {
                throw new AssertionError("RBBQ native exact and failed");
            }
        }

        try (
                RbbqLongQueue first = new RbbqLongQueue(64, 8);
                RbbqLongQueue second = new RbbqLongQueue(64, 8);
                RbbqLongQueue third = new RbbqLongQueue(64, 8)) {
            RbbqLongQueue.pushRangeToThreeExact(first, second, third, 1, 3);
            if (first.popAddExact(3) != 6 || second.popSubExact(3) != -6 || third.popAndExact(3) != 0) {
                throw new AssertionError("RBBQ native exact three-queue range failed");
            }
        }
    }
}
