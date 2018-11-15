package bdma.ulb.datamining.model;

import bdma.ulb.datamining.util.Assert;
import bdma.ulb.datamining.util.Util;

import java.util.List;
import java.util.Objects;

public class Grid {

    private final List<double[]> dataPoints;
    private final GridLabel label;
    private final String id;
    private final GridCornerPoints cornerPoints;

    public Grid(
            final List<double[]> dataPoints,
            final GridLabel label,
            final String id,
            final GridCornerPoints cornerPoints
    )
    {
        Assert.notNull(label, "label cannot be null");
        Assert.notNull(label, "cornerPoints cannot be null");

        this.dataPoints = dataPoints;
        this.label = label;
        this.id = id;
        this.cornerPoints = cornerPoints;
    }

    public void add(final double[] point) {
        dataPoints.add(point);
    }

    public int getCount() {
        return dataPoints.size();
    }

    public List<double[]> getDataPoints() {
        return dataPoints;
    }

    public GridLabel getLabel() {
        return label;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Grid {" +
                "dataPoints=" + Util.stringRepresentation(dataPoints) +
                ", label=" + label +
                ", id='" + id + '\'' +
                ", cornerPoints=" + cornerPoints +
                '}' + ", SIZE = " + getCount();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Grid grid = (Grid) o;
        return Objects.equals(id, grid.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
