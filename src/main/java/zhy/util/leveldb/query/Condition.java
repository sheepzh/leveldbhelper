package zhy.util.leveldb.query;

import java.util.Map;
import java.util.Objects;

import static zhy.util.leveldb.query.Operation.*;

/**
 * Condition of query
 */
public class Condition {

    private int condition = 0;

    private String targetValue1;
    private String targetValue2;
    private String targetParam;

    public Condition(String targetParam, String targetValue, Operation... cond) {
        this.targetParam = targetParam;
        this.targetValue1 = targetValue;
        for (Operation operation : cond) condition |= operation.mask();
    }

    public Condition(String targetParam, String targetValue1, String targetValue2, Operation... cond) {
        this.targetParam = targetParam;
        this.targetValue1 = targetValue1;
        this.targetValue2 = targetValue2;
        for (Operation operation : cond) condition |= operation.mask();
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

    private boolean need(Operation sub) {
        return sub.need(condition);
    }
}
