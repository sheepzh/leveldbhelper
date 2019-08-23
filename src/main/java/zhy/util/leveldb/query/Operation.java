package zhy.util.leveldb.query;

/**
 * Operation of query condition.
 *
 * @author zhanghengyang
 * @since 1.0
 */
public enum Operation {
    /**
     * Numeric condition
     */
    NUMBER_NOT_LOWER,
    NUMBER_NOT_GREATER,
    NUMBER_LOWER,
    NUMBER_GREATER,
    NUMBER_BETWEEN,
    /**
     * String condition
     */
    START,
    END,
    CONTAIN,
    NOT_CONTAIN,
    /**
     * Object condition
     */
    EQUAL,
    NOT_EQUAL;

    private int mask;

    Operation() {
        mask = 1 << this.ordinal();
    }

    /**
     * @param target mask
     * @return true, need this condition;
     */
    public boolean need(int target) {
        return (mask & target) != 0;
    }

    public int mask() {
        return mask;
    }
}
