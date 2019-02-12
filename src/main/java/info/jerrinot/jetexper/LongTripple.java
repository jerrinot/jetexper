package info.jerrinot.jetexper;

import java.io.Serializable;

public final class LongTripple implements Serializable {
    private long left;
    private long middle;
    private long right;

    public LongTripple() {

    }

    public LongTripple(LongTripple otherTripple) {
        this.left = otherTripple.getLeft();
        this.middle = otherTripple.getMiddle();
        this.right = otherTripple.getRight();
    }

    public Long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        this.left = left;
    }

    public Long getRight() {
        return right;
    }

    public void setRight(Long right) {
        this.right = right;
    }

    public long getMiddle() {
        return middle;
    }

    public void setMiddle(long middle) {
        this.middle = middle;
    }


    public void merge(LongTripple other) {
        left += other.getLeft();
        right += other.getRight();
        middle += other.getMiddle();
    }

    @Override
    public String toString() {
        long delta = Math.abs(left - right);
        long max = Math.max(left, right);
        double error = (double)delta / max * 100;

        return "LongTuple{"
                + "left=" + left
                + ", right=" + right
                + ", delta=" + delta
                + ", error=" + String.format("%.2f", error) + '%'
                + ", middle=" + middle
                + '}';
    }
}
