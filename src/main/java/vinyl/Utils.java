package vinyl;

import java.util.Comparator;

class Utils {
    @SuppressWarnings("unchecked")
    static <T> T cast(Object o) {
        return (T) o;
    }
    
    /**
     * Natural order, nulls first/lowest.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Comparator<Object> DEFAULT_COMPARATOR = (a, b) -> {
        if (a == b)
            return 0;
        if (a == null)
            return -1;
        if (b == null)
            return 1;
        return ((Comparable) a).compareTo(b);
    };
    
    /**
     * Only safe when Records have the same Header and all Fields are Comparable.
     */
    static final Comparator<Record> HEADLESS_RECORD_COMPARATOR = (a, b) -> {
        for (int i = 0; i < a.values.length; i++) {
            int v = DEFAULT_COMPARATOR.compare(a.values[i], b.values[i]);
            if (v != 0) {
                return v;
            }
        }
        return 0;
    };
    
    static Field<?> tempField() {
        return new Field<>("<anonymous>");
    }
}
