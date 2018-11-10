package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.ComplexGrid;
import bdma.ulb.datamining.model.Grid;
import bdma.ulb.datamining.model.GridLabel;
import bdma.ulb.datamining.util.Assert;
import bdma.ulb.datamining.util.Numbers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static bdma.ulb.datamining.util.Numbers.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.*;

public class ParallelDbScan {

    private static final Logger logger = LoggerFactory.getLogger(ParallelDbScan.class);
    private static final int cores = Runtime.getRuntime().availableProcessors();
    private static final int workers = cores;  //For now
    private static final ExecutorService executor = Executors.newFixedThreadPool(workers);

    private final List<double[]> dataSet;
    private final double epsilon;
    private final int minPts;
    private final int partitions;

    public ParallelDbScan(
            final List<double[]> dataSet,
            final double epsilon,
            final int minPts,
            final int partitions
    )
    {
        Assert.notNull(dataSet, "dataSet cannot be null");
        Assert.isTrue(epsilon > ZERO, "epsilon must be a double value greater than 0");
        Assert.isTrue(minPts > ZERO, "minPts must be an integer greater than one");
        Assert.isTrue(partitions > ZERO, "minPts must be an integer greater than one");

        this.dataSet = dataSet;
        this.epsilon = epsilon;
        this.minPts = minPts;
        this.partitions = partitions;
    }


    public List<List<double[]>> compute() {
        // First, divide the dataset into "partitions" number of grids
        final List<Grid> grids = splitIntoGrids(dataSet);

        final List<ComplexGrid> nonDenseComplexGrids = grids.stream()
                                                            .filter(grid -> grid.getLabel() == GridLabel.NOT_DENSE)
                                                            .map(grid -> new ComplexGrid(singletonList(grid)))
                                                            .collect(toList());
        final List<Grid> denseGrids = grids.stream()
                                              .filter(grid -> grid.getLabel() == GridLabel.DENSE)
                                              .collect(toList());

        //Multi threaded DBScan

        try {
            executor.shutdown();
        }
        catch(final Exception ex) {
            logger.error("" ,ex);
        }
        return null;
    }

    private List<Grid> splitIntoGrids(final List<double[]> dataSet) {
        final List<Grid> grids = new ArrayList<>();
        final int blocks = Math.round(dataSet.size() / workers);
        final int x = getWholeDataSetSize() / workers;
        final int y = 2 * x ;
        for(int i = 1; i <= blocks; i++) {
            final List<double[]> subset = dataSet.subList(blocks * (i - 1), (blocks * i) - 1);
            final int gridSize = subset.size();
            if(gridSize > (getWholeDataSetSize() / workers)) {
                grids.addAll(splitIntoGrids(subset));
            }
            //TODO : Work on this
            else if (gridSize > (2 )) {
                grids.add(new Grid(subset, GridLabel.DENSE, i));
            }
            else {
                grids.add(new Grid(subset, GridLabel.NOT_DENSE, i));
            }
        }
        return grids;
    }

    private int getWholeDataSetSize() {
        return dataSet.size();
    }

}
