package zhy.util.leveldb.query;

import java.util.Map;
import java.util.Objects;

import static zhy.util.leveldb.query.Operation.*;

/**
 * Condition of query
 *
 * @author zhanghengyang
 * @since 1.0
 */
public class Condition {

    private int condition = 0;

    private String val1;
    private String val2;
    private String targetParam;

    public Condition(String targetParam, String targetValue, Operation... cond) {
        this.targetParam = targetParam;
        this.val1 = targetValue;
        for (Operation operation : cond) condition |= operation.mask();
    }

    public Condition(String targetParam, String val1, String val2, Operation... cond) {
        this.targetParam = targetParam;
        this.val1 = val1;
        this.val2 = val2;
        for (Operation operation : cond) condition |= operation.mask();
    }

    public boolean satisfies(Map<String, Object> map) {
        Object target = map.get(targetParam);
        if (target == null) return false;
        String targetStr = target.toString();
        boolean result = true;
        if (need(NUMBER_LOWER)) {
            try {
                result = Double.valueOf(targetStr) < Double.valueOf(val1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_GREATER)) {
            try {
                result = Double.valueOf(targetStr) > Double.valueOf(val1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_NOT_LOWER)) {
            try {
                result = Double.valueOf(targetStr) >= Double.valueOf(val1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(NUMBER_NOT_GREATER)) {
            try {
                result = Double.valueOf(targetStr) <= Double.valueOf(val1);
            } catch (Exception e) {
                result = false;
            }
        }
        if (result && need(EQUAL)) result = Objects.equals(val1, targetStr);
        if (result && need(NOT_EQUAL)) result = !Objects.equals(val1, targetStr);
        if (result && need(START))
            result = targetStr != null && val1 != null && targetStr.startsWith(val1);
        if (result && need(END)) result = targetStr != null && val1 != null && targetStr.endsWith(val1);
        if (result && need(CONTAIN))
            result = targetStr != null && val1 != null && targetStr.contains(val1);
        if (result && need(NOT_CONTAIN))
            result = targetStr != null && val1 != null && !targetStr.contains(val1);
        if (result && need(NUMBER_BETWEEN)) {
            try {
                double d1 = Double.valueOf(val1), d2 = Double.valueOf(val2), targetD = Double.valueOf(targetStr);
                result = d1 <= targetD && d2 >= targetD;
            } catch (Exception e) {
                result = false;
            }
        }
        return result;
    }


    public int getCondition() {
        return condition;
    }

    public Condition setCondition(int condition) {
        this.condition = condition;
        return this;
    }

    public String getVal1() {
        return val1;
    }

    public Condition setVal1(String val1) {
        this.val1 = val1;
        return this;
    }

    private boolean need(Operation sub) {
        return sub.need(condition);
    }
}
