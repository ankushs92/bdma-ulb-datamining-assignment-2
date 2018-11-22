package bdma.ulb.datamining.model;

import bdma.ulb.datamining.util.Assert;
import bdma.ulb.datamining.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

public class Grid {

    private final List<double[]> dataPoints;
    private final List<double[]> extendedPoints;
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
        this.extendedPoints = new ArrayList<>();
    }

    public void addExtendedPoints(final double[] point) {
        extendedPoints.add(point);
    }

    public List<double[]> getAllPoints() {
        final List<double[]> points = new ArrayList<>(dataPoints);
        points.addAll(extendedPoints);
        return points.stream().distinct().collect(toList());
    }

    public int getCount() {
        return (int) getAllPoints().stream().distinct().count();
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

    public GridCornerPoints getCornerPoints() {
        return cornerPoints;
    }

    public boolean isDense() {
        return label == GridLabel.DENSE;
    }

    @Override
    public String toString() {
        return "Grid {" +
                "dataPoints=" + Util.stringRepresentation(getAllPoints()) +
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
