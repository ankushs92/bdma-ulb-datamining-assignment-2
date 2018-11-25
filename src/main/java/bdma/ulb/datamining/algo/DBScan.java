package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.Cluster;
import bdma.ulb.datamining.model.Label;
import bdma.ulb.datamining.util.Assert;

import java.util.*;

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
        final Map<double[], Label> visitedPoints = new HashMap<>();
        for(final double[] point : dataSet) {
            //Only unvisited points
            if(isNull(visitedPoints.get(point))) {
                final List<double[]> neighbours = getNeighbours(point, dataSet, epsilon);
                //Core point
                if(neighbours.size() >= minPts) {
                    final List<double[]> cluster = new ArrayList<>();
                    clusters.add(new Cluster(expandCluster(dataSet, point, neighbours, cluster, visitedPoints)));
                }
                else {
                    visitedPoints.put(point, Label.NOISE);
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
            final Map<double[], Label> visitedPoints
    )
    {
        cluster.add(point);
        visitedPoints.put(point, Label.PART_OF_CLUSTER);

        List<double[]> seeds = new ArrayList<>(neighbours);
        int index = 0;
        //While loop used to escape Java's ConcurrentModificationException
        while(index < seeds.size()) {
            final double[] currentPoint = seeds.get(index);
            final Label label = visitedPoints.get(currentPoint);
            if(isNull(label)) {
                final List<double[]> currentNeighbourhood = getNeighbours(currentPoint, dataSet, epsilon);
                if(currentNeighbourhood.size() >= minPts) {
                    //Grow the cluster
                    seeds = merge(seeds, currentNeighbourhood);
                }
            }
            if(label != Label.PART_OF_CLUSTER) {
                visitedPoints.put(currentPoint, Label.PART_OF_CLUSTER);
                cluster.add(currentPoint);
            }
            index ++;
        }
        return cluster;
    }


    private List<double[]> merge(final List<double[]> one, final List<double[]> two) {
        final Set<double[]> oneSet = new HashSet<>(one);
        for (final double[] point : two) {
            if (!oneSet.contains(point)) {
                one.add(point);
            }
        }
        return one;
    }


}
