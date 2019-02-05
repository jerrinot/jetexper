package info.jerrinot.jetexper;

import java.io.Serializable;

public final class LongTuple implements Serializable {
    private long left;
    private long right;

    public LongTuple() {

    }

    public LongTuple(LongTuple otherTuple) {
        this.left = otherTuple.getLeft();
        this.right = otherTuple.getRight();
    }

    public Long getLeft() {
        return left;
    }

    public void setLeft(Long left) {
        this.left = left;
    }

    public Long getRight() {
        return right;
    }

    public void setRight(Long right) {
        this.right = right;
    }

    public void merge(LongTuple other) {
        left += other.left;
        right += other.right;
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
                + '}';
    }
}
