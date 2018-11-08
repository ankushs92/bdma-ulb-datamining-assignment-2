import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public class DBScan {

    private static final double ZERO = 0.0;

    private final double epsilon;
    private final int minPts;
    private final List<double[]> dataSet;

    public DBScan(
            final List<double[]> dataSet,
            final double epsilon,
            final int minPts
    )
    {
        Assert.notNull(dataSet, "dataSet cannot be null");
        if(epsilon <= ZERO) {
            throw new IllegalArgumentException("epsilon must be a double value greater than 0");
        }
        if(minPts < ZERO) {
            throw new IllegalArgumentException("minPts must be an integer greater than one");
        }
        this.dataSet = dataSet;
        this.epsilon = epsilon;
        this.minPts = minPts;
    }

    public List<List<double[]>> compute() {
        final List<List<double[]>> clusters = new ArrayList<>();
        final Map<double[], Label> visited = new HashMap<>();
        for(final double[] point : dataSet) {
            //Only unvisited points
            if(isNull(visited.get(point))) {
                final List<double[]> neighbours = getNeighbours(point, dataSet);
                //Core point
                if(neighbours.size() >= minPts) {
                    final List<double[]> cluster = new ArrayList<>();
                    clusters.add(expandCluster(dataSet, point, neighbours, cluster, visited));
                }
                else {
                    visited.put(point, Label.NOISE);
                }
            }
        }
        return clusters;
    }

//    dataSet, point, cluster, neighbourhood, epsilon, minPts
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
                final List<double[]> currentNeighbourhood = getNeighbours(current, dataSet);
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


    private List<double[]> getNeighbours(final double[] point, final List<double[]> dataSet) {
        final List<double[]> neighbours = new ArrayList<>();
        //All those points whose euclidean distance from 'point' is less than epsilon are in nbd
        dataSet.forEach(dataPoint -> {
            //Don't include the point in the neighbourhood
            if(!Arrays.equals(point, dataPoint)) {
                final double distance = computeEuclideanDistance(point, dataPoint);
                if(distance <= epsilon) {
                    neighbours.add(dataPoint);
                }
            }
        });
        return neighbours;
    }

    private static double computeEuclideanDistance(final double[] point1, final double[] point2) {
        double sum = ZERO;
        for(int index = 0; index < point1.length; index++) {
            sum += Math.pow(point1[index] - point2[index], 2);
        }
        return Math.sqrt(sum);
    }

    private List<double[]> merge(final List<double[]> one, final List<double[]> two) {
        final List<double[]> result = new ArrayList<>(one);
        result.addAll(two);
        return result.stream().distinct().collect(Collectors.toList());
    }

}
