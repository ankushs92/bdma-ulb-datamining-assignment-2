package bdma.ulb.datamining.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Util {



    public static List<String> stringRepresentation(final List<double[]> points) {
        return points.stream()
                .map(Arrays::toString)
                .collect(Collectors.toList());
    }

    public static boolean isNullOrEmpty(final Collection<?> collection) {
        if(Objects.isNull(collection) || collection.isEmpty()) {
            return true;
        }
        return false;
    }


}
