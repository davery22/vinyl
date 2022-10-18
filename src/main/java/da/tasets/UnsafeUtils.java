package da.tasets;

import java.util.Comparator;

class UnsafeUtils {
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
     * Only safe when Rows have the same Header and all Columns are Comparable.
     */
    static final Comparator<Row> HEADLESS_ROW_COMPARATOR = (a, b) -> {
        for (int i = 0; i < a.data.length; i++) {
            int v = DEFAULT_COMPARATOR.compare(a.data[i], b.data[i]);
            if (v != 0) {
                return v;
            }
        }
        return 0;
    };
}
