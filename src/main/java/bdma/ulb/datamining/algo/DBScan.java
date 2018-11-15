package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.Cluster;
import bdma.ulb.datamining.model.Label;
import bdma.ulb.datamining.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static bdma.ulb.datamining.util.Numbers.ZERO;
import static java.util.Objects.isNull;

public class DBScan implements IDbScan {

    private final List<double[]> dataSet;
    private final double epsilon;
    private final int minPts;

    public DBScan(
            final List<double[]> dataSet,
            final double epsilon,
            final int minPts
    )
    {
        Assert.notNull(dataSet, "dataSet cannot be null");
        Assert.isTrue(epsilon > ZERO, "epsilon must be a value greater than 0");
        Assert.isTrue(minPts > ZERO, "minPts must be an integer greater than one");
        this.dataSet = dataSet;
        this.epsilon = epsilon;
        this.minPts = minPts;
    }

    @Override
    public List<Cluster> compute() {
        final List<Cluster> clusters = new ArrayList<>();
        final Map<double[], Label> visited = new HashMap<>();
        for(final double[] point : dataSet) {
            //Only unvisited points
            if(isNull(visited.get(point))) {
                final List<double[]> neighbours = getNeighbours(point, dataSet, epsilon);
                //Core point
                if(neighbours.size() >= minPts) {
                    final List<double[]> cluster = new ArrayList<>();
                    clusters.add(new Cluster(expandCluster(dataSet, point, neighbours, cluster, visited)));
                }
                else {
                    visited.put(point, Label.NOISE);
                }
            }
        }
        return clusters;
    }

    private List<double[]> expandCluster(
            final List<double[]> dataSet,
            final double[] point,
            final List<double[]> neighbours,
            final List<double[]> cluster,
            final Map<double[], Label> visited
    )
    {
        cluster.add(point);
        visited.put(point, Label.PART_OF_CLUSTER);

        List<double[]> seeds = new ArrayList<>(neighbours);
        int index = 0;
        while(index < seeds.size()) {
            final double[] current = seeds.get(index);
            final Label label = visited.get(current);
            if(isNull(label)) {
                final List<double[]> currentNeighbourhood = getNeighbours(current, dataSet, epsilon);
                if(currentNeighbourhood.size() >= minPts) {
                    seeds = merge(seeds, currentNeighbourhood);
                }
            }
            if(label != Label.PART_OF_CLUSTER) {
                visited.put(current, Label.PART_OF_CLUSTER);
                cluster.add(current);
            }
            index ++;
        }
        return cluster;
    }


    private List<double[]> merge(final List<double[]> one, final List<double[]> two) {
        final List<double[]> result = new ArrayList<>(one);
        result.addAll(two);
        return result.stream().distinct().collect(Collectors.toList());
    }

}
