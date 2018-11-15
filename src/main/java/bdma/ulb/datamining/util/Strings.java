package bdma.ulb.datamining.util;

import java.util.Objects;

public class Strings {

    public static boolean hasText(final String str) {
        if (Objects.isNull(str)) {
            return false;
        }
        final int length = str.length();
        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }


}
