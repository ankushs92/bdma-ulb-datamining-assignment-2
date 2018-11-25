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


    @Override
    public String toString() {
        return "GridCornerPoints{" +
                "id=" + gridId +
                ", xAxisCornerPoints=" + xAxisCornerPoints +
                ", yAxisCornerPoints=" + yAxisCornerPoints +
                '}';
    }
}
