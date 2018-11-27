package bdma.ulb.datamining.model;

import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class Cluster {

    private final List<double[]> dataPoints;

    public Cluster(final List<double[]> dataPoints) {
        this.dataPoints = dataPoints;
    }

    public List<double[]> getDataPoints() {
        return dataPoints.stream().distinct().collect(Collectors.toList());
    }

    public int getSize() {
        return dataPoints.stream().distinct().collect(toList()).size();
    }

    public void addDataPoints(final List<double[]> morePoints) {
        dataPoints.addAll(morePoints);
    }

    public Set<double[]> getCommonPoints(final Cluster otherCluster) {
        final List<double[]> otherDataPoints = otherCluster.getDataPoints();
        return (Set<double[]>) CollectionUtils.intersection(dataPoints, otherDataPoints).stream().collect(Collectors.toSet());
    }

    public void remove(final double[] point) {
        dataPoints.removeAll(Arrays.asList(point));
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return Objects.equals(dataPoints, cluster.dataPoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataPoints);
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "dataPoints=" + dataPoints +
                '}';
    }

}
