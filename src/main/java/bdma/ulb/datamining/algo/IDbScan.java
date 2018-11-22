package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.Cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bdma.ulb.datamining.util.Numbers.ZERO;

public interface IDbScan {

    List<Cluster> compute() throws Exception;

    default List<double[]> getNeighbours(final double[] point, final List<double[]> dataSet, final double epsilon) {
        final List<double[]> neighbours = new ArrayList<>();
        //All those points whose euclidean distance from 'point' is less than epsilon are in nbd
        dataSet.forEach(dataPoint -> {
            final double distance = computeEuclideanDistance(point, dataPoint);
            if(distance <= epsilon) {
                neighbours.add(dataPoint);
            }
        });
        return neighbours;
    }

    default double computeEuclideanDistance(final double[] point1, final double[] point2) {
        double sum = ZERO;
        for(int index = 0; index < point1.length; index++) {
            sum += Math.pow(point1[index] - point2[index], 2);
        }
        return Math.sqrt(sum);
    }


}
