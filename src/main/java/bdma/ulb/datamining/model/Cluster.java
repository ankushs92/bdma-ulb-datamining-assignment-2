package bdma.ulb.datamining.model;

import java.util.List;

public class Cluster {

    private final List<double[]> dataPoints;

    public Cluster(final List<double[]> dataPoints) {
        this.dataPoints = dataPoints;
    }

    public List<double[]> getDataPoints() {
        return dataPoints;
    }

    public int getSize() {
        return dataPoints.size();
    }

}
