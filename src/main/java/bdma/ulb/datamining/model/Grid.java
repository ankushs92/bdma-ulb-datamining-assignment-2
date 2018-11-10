package bdma.ulb.datamining.model;

import bdma.ulb.datamining.util.Assert;

import java.util.List;

public class Grid {

    private final List<double[]> dataPoints;
    private final GridLabel label;
    private final int id;

    public Grid(
            final List<double[]> dataPoints,
            final GridLabel label,
            final int id
    )
    {
        Assert.notNull(dataPoints, "dataPoints cannot be null");
        Assert.notNull(label, "label cannot be null");

        this.dataPoints = dataPoints;
        this.label = label;
        this.id = id;
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

    public int getId() {
        return id;
    }
}
