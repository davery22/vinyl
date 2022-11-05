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
    
    static Field<?> tempField() {
        return new Field<>("<anonymous>");
    }
}
