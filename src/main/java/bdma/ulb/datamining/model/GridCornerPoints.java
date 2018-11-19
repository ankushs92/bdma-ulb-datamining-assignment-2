package bdma.ulb.datamining.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GridCornerPoints {

    private final String gridId;
    private final RightOpenInterval xAxisCornerPoints;
    private final RightOpenInterval yAxisCornerPoints;

    public GridCornerPoints(
            final String gridId,
            final RightOpenInterval xAxisCornerPoints,
            final RightOpenInterval yAxisCornerPoints
    )
    {
        this.gridId = gridId;
        this.xAxisCornerPoints = xAxisCornerPoints;
        this.yAxisCornerPoints = yAxisCornerPoints;
    }

    public RightOpenInterval getxAxisCornerPoints() {
        return xAxisCornerPoints;
    }

    public RightOpenInterval getyAxisCornerPoints() {
        return yAxisCornerPoints;
    }

    public String getGridId() {
        return gridId;
    }

    public List<Coordinate> getBoundaries() {
        final double minX = xAxisCornerPoints.getStart();
        final double maxX = xAxisCornerPoints.getEnd();
        final double minY = yAxisCornerPoints.getStart();
        final double maxY = yAxisCornerPoints.getEnd();

        return Arrays.asList(
                new Coordinate(minX, minY),
                new Coordinate(minX, maxY),
                new Coordinate(maxX, minY),
                new Coordinate(maxX, maxY)
        );
    }

    public boolean hasAnyBoundaryPointCommon(final GridCornerPoints gridCornerPoints) {
        final List<Coordinate> otherGridBoundaries = gridCornerPoints.getBoundaries();
        final List<Coordinate> boundaries = getBoundaries();
        boolean result = false;
        for(final Coordinate otherGridCoordinate : otherGridBoundaries) {
            for(final Coordinate gridCoordinate : boundaries) {
                if(Objects.equals(otherGridCoordinate, gridCoordinate)) {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "GridCornerPoints{" +
                "id=" + gridId +
                ", xAxisCornerPoints=" + xAxisCornerPoints +
                ", yAxisCornerPoints=" + yAxisCornerPoints +
                '}';
    }
}
