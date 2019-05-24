package zhy.util.leveldb.query;

import com.alibaba.fastjson.JSON;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static zhy.util.leveldb.query.Operation.EQUAL;
import static zhy.util.leveldb.query.Operation.NOT_EQUAL;

/**
 * Filter of conditions.
 *
 * @author zhanghengyang
 * @since 1.0
 */
public class ConditionFilter {
    /**
     * Whether the param of value satisfies the condition list.
     * If conditions is empty,it always returns true.
     *
     * @param value      Value string to judge
     * @param conditions Conditions
     * @return <code>true</code> if target value satisfies all conditions in the list.
     */
    public boolean satisfies(String value, List<Condition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }
        Map<String, Object> map = JSON.parseObject(value);
        if (map == null) {
            //return false if any some positive condition exists.
            return conditions.stream()
                    .map(Condition::getCondition)
                    .noneMatch(c -> c != NOT_EQUAL.mask() && c != Operation.NOT_CONTAIN.mask());
        }
        return conditions.stream()
                .map(c -> c.satisfies(map))
                .reduce((a, b) -> a & b)
                .orElse(false);
    }

    public static List<Condition> generateCondition(Object entity) {
        List<Condition> conditions = new LinkedList<>();
        if (entity == null) return conditions;
        for (Field field : entity.getClass().getDeclaredFields()) {
            if (Modifier.isFinal(field.getModifiers())) continue;
            field.setAccessible(true);
            try {
                String name = field.getName();
                Object value = field.get(entity);
                if (value != null && !"id".equals(name)) {
                    conditions.add(new Condition(name, value.toString(), EQUAL));
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return conditions;
    }
}
