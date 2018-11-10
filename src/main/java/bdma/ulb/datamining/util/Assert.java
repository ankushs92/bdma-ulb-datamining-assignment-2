package bdma.ulb.datamining.util;

import java.util.Objects;

public class Assert {

    public static <T> void notNull(final T t, final String errorMsg) {
        if(Objects.isNull(t)) {
            throw new IllegalArgumentException(errorMsg);
        }
    }

    public static void isTrue(final boolean expression, final String errorMsg) {
        if(!expression) {
            throw new IllegalArgumentException(errorMsg);
        }
    }

}
