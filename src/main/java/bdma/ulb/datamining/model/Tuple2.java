package bdma.ulb.datamining.model;

import java.util.Arrays;

public class Tuple2 {

    private final double[] point1;
    private final double[] point2;

    public Tuple2(final double[] point1, final double[] point2) {
        this.point1 = point1;
        this.point2 = point2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple2 tuple2 = (Tuple2) o;
        return Arrays.equals(point1, tuple2.point1) &&
                Arrays.equals(point2, tuple2.point2);
    }

    @Override
    public int hashCode() {

        int result = Arrays.hashCode(point1);
        result = 31 * result + Arrays.hashCode(point2);
        return result;
    }
}
