package bdma.ulb.datamining.model;

import java.util.List;
import java.util.stream.Collectors;

public class Cluster {

    private final List<double[]> dataPoints;

    public Cluster(final List<double[]> dataPoints) {
        this.dataPoints = dataPoints;
    }

    public List<double[]> getDataPoints() {
        return dataPoints;
    }

    public int getSize() {
        return dataPoints.stream().distinct().collect(Collectors.toList()).size();
    }


    public boolean doesPointBelongToCluster(final double[] points) {
        boolean result = false;
        if(dataPoints.contains(points)) {
            result = true;
        }
        return result;
    }

}
