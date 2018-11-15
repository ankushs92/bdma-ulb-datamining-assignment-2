package bdma.ulb.datamining.model;

/**
 * An interval is Right open if the right point of the interval is not a part of it. For example, [5,7) contains
 * all those points that are greater than equal to 5, but strictly less than 7
 */
public class RightOpenInterval {

    private final double start;
    private final double end;

    public RightOpenInterval(final double start, final double end) {
        this.start = start;
        this.end = end;
    }


    public double getStart() {
        return start;
    }

    public double getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "" +
                "[" + start +
                "," + end +
                ')';
    }
}
