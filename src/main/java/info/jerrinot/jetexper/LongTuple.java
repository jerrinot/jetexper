package info.jerrinot.jetexper;

import java.io.Serializable;

public final class LongTuple implements Serializable {
    private Long left;
    private Long right;

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
        if (left == null) {
            left = other.left;
        } else if (other.left != null) {
            left += other.left;
        }

        if (right == null) {
            right = other.right;
        } else if (other.right != null) {
            right += other.right;
        }
    }

    @Override
    public String toString() {
        return "LongTuple{" +
                "left=" + left +
                ", right=" + right +
                ", delta=" + Math.abs(left - right)+
                '}';
    }
}
