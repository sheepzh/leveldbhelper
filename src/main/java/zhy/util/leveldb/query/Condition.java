package zhy.util.leveldb.query;

import java.util.Map;
import java.util.Objects;

/**
 * Condition of query
 */
public class Condition {

    /**
     * Compare
     */
    public final static int NUMBER_NOT_LOWER = 1;
    public final static int NUMBER_NOT_GREATER = 2;
    public final static int NUMBER_LOWER = 4;
    public final static int NUMBER_GREATER = 8;
    public final static int EQUAL = 16;
    public final static int START = 32;
    public final static int END = 64;
    public final static int CONTAIN = 128;
    public final static int NOT_CONTAIN = 256;
    public final static int NOT_EQUAL = 512;

    private int condition = 0;

    private String targetValue1;
    private String targetValue2;
    private String targetParam;

    public Condition(String targetParam, String targetValue, int... cond) {
        this.targetParam = targetParam;
        this.targetValue1 = targetValue;
        for (int c : cond) condition |= c;
    }

    public Condition(String targetParam, String targetValue1, String targetValue2, int... cond) {
        this.targetParam = targetParam;
        this.targetValue1 = targetValue1;
        this.targetValue2 = targetValue2;
        for (int c : cond) condition |= c;
    }

    public boolean satisfies(Map<String, Object> map) {
        Object target = map.get(targetParam);
        if (target == null) return false;
        String targetStr = target.toString();
        boolean result = true;
        if (need(NUMBER_LOWER)) {
            try {
                result = Double.valueOf(targetStr) < Double.valueOf(targetValue1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_GREATER)) {
            try {
                result = Double.valueOf(targetStr) > Double.valueOf(targetValue1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_NOT_LOWER)) {
            try {
                result = Double.valueOf(targetStr) >= Double.valueOf(targetValue1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_NOT_GREATER)) {
            try {
                result = Double.valueOf(targetStr) <= Double.valueOf(targetValue1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(EQUAL)) result = Objects.equals(targetValue1, targetStr);
        if (result && need(NOT_EQUAL)) result = !Objects.equals(targetValue1, targetStr);
        if (result && need(START))
            result = targetStr != null && targetValue1 != null && targetStr.startsWith(targetValue1);
        if (result && need(END)) result = targetStr != null && targetValue1 != null && targetStr.endsWith(targetValue1);
        if (result && need(CONTAIN))
            result = targetStr != null && targetValue1 != null && targetStr.contains(targetValue1);
        if (result && need(NOT_CONTAIN))
            result = targetStr != null && targetValue1 != null && !targetStr.contains(targetValue1);
        return result;
    }


    public int getCondition() {
        return condition;
    }

    public Condition setCondition(int condition) {
        this.condition = condition;
        return this;
    }

    public String getTargetValue1() {
        return targetValue1;
    }

    public Condition setTargetValue1(String targetValue1) {
        this.targetValue1 = targetValue1;
        return this;
    }

    private boolean need(int sub) {
        return (condition & sub) != 0;
    }
}
