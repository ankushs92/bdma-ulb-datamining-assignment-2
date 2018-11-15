package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.*;
import bdma.ulb.datamining.util.Assert;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static bdma.ulb.datamining.model.GridLabel.DENSE;
import static bdma.ulb.datamining.model.GridLabel.NOT_DENSE;
import static bdma.ulb.datamining.util.Numbers.ZERO;
import static bdma.ulb.datamining.util.Util.isNullOrEmpty;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class ParallelDBScan implements IDbScan {

    private static final Logger log = LoggerFactory.getLogger(ParallelDBScan.class);
    private static final int cores = Runtime.getRuntime().availableProcessors();
    private static final int workers = cores;  //For now
    private static final ExecutorService executor = Executors.newFixedThreadPool(workers);

    private final List<double[]> dataSet;
    private final double epsilon;
    private final int minPts;
    private final int partitions;

    public ParallelDBScan(
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

    @Override
    public List<Cluster> compute() throws Exception {
        // First, divide the dataset into "partitions" number of grids
        final List<Grid> grids = splitIntoGrids(dataSet, partitions);

        //Step 4 of main algorithm. We calculate epsilon nbd of every point, and build an adjacency list
        final Multimap<String, Grid> adjacencyList =  HashMultimap.create();
        for(final Grid grid : grids) {
            final String gridId = grid.getId();
            final List<double[]> points = grid.getDataPoints();
            for(final double[] point : points) {
                final List<double[]> neighbours = getNeighbours(point, dataSet, epsilon);
                if (!isNullOrEmpty(neighbours)) {
                    for(final double[] neighbour : neighbours) {
                        final Grid neighbourPointGrid = grids.stream()
                                                             .filter( it -> it.getDataPoints().stream().anyMatch(i -> Arrays.equals(neighbour, i)))
                                                             .findFirst().get();
//                        System.out.println("Is neighhourhood grid equal to current grid? " + Objects.equals(neighbourPointGrid, grid));
                        if(!Objects.equals(neighbourPointGrid, grid)) {
                            System.out.println("neighbourPointGrid " + neighbourPointGrid);
                            System.out.println("Grid id " + gridId);
                            adjacencyList.put(gridId, neighbourPointGrid);
                        }
                    }
                }
            }
        }


        for (Entry<String, Grid> stringGridEntry : adjacencyList.entries()) {
            System.out.println(stringGridEntry);
        }

        final List<ComplexGrid> nonDenseComplexGrids = grids.stream()
                                                            .filter(grid -> grid.getLabel() == NOT_DENSE)
                                                            .map(grid -> new ComplexGrid(singletonList(grid)))
                                                            .collect(toList());


        //This is wrong
        final List<ComplexGrid> denseComplexGrids = grids.stream()
                                                         .filter(grid -> grid.getLabel() == DENSE)
                                                         .collect(Collectors.groupingBy(Grid::getId))
                                                         .entrySet()
                                                         .stream()
                                                         .map(entry -> new ComplexGrid(entry.getValue()))
                                                         .collect(toList());


        //Multi threaded DBScan
        final List<ComplexGrid> allComplexGrids = new ArrayList<>(nonDenseComplexGrids);
        allComplexGrids.addAll(denseComplexGrids);

        final List<Future<List<Cluster>>> resultPool = new ArrayList<>();
        for(final ComplexGrid complexGrid : allComplexGrids) {
            final Future<List<Cluster>> result = executor.submit( () -> {
                List<double[]> dataSet = complexGrid.getGrids()
                                                    .stream()
                                                    .map(Grid::getDataPoints)
                                                    .flatMap(Collection::stream)
                                                    .collect(Collectors.toList());
                final DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
                final List<Cluster> clusters = dbScan.compute();
                return clusters;
            });
            resultPool.add(result);
        }

        //Merging step
        for(final Future<List<Cluster>> result : resultPool) {
            final List<Cluster> cluster = result.get();
        }



        try {
            executor.shutdown();
        }
        catch(final Exception ex) {
            log.error("" ,ex);
        }
        return null;
    }

//    private static final int numOfPartitions = 8;

    private static final double DELTA = 0.01;

    private static List<Grid> splitIntoGrids(final List<double[]> dataSet, final int partitions) {
        final int dataSetSize = dataSet.size();
        log.debug("Total points in DataSet {}", dataSetSize);
        //We find out whether the dataset is of 1 dimension, 2 or n dimension. We only need to find any one element of the dataset, and calculate it size
        final int dimension = dataSet.get(0).length;

        log.info("Dimension of DataSet {}", dimension);
        //We define a Projection as follows : if we are in R2, then projection of Y axis on X by setting y = 0 in all (x,y) pairs
        final Map<Integer, List<Double>> projections = new HashMap<>();
        for(int i = 0; i < dimension; i ++) {
            final int currentDimension = i + 1  ; //Had to do this to escape Lambda's "effectively final" compilation error.
            log.info("Current Dimension {}", currentDimension);
            final List<Double> projection = dataSet.stream().map( points -> points[currentDimension - 1]).collect(toList());
            projections.put(currentDimension, projection);
        }

        final Map<Integer, List<RightOpenInterval> > dimensionsStrides = new HashMap<>();
        projections.forEach((key, value) -> {
            final int currentDimension = key;
            final List<Double> projectionSorted = value.stream().sorted().collect(toList());
            final int size = projectionSorted.size();
            final double min = projectionSorted.get(0);
            final double max = projectionSorted.get(size - 1);
            final double strideSize = strideSize(min, max, partitions) + DELTA;
            log.info("Stride Size {}", strideSize);

            final List<RightOpenInterval> intervals = new ArrayList<>();
            for(int i = 1; i <= partitions; i ++) {
                final double right =  min  + (i * strideSize);
                final double left = min + ( (i - 1) * strideSize);
                intervals.add(new RightOpenInterval(left, right));
            }
            dimensionsStrides.put(currentDimension, intervals);
        });

        final List<GridCornerPoints> cornerPointsList = new ArrayList<>();
        int id = 0;
        for(final Entry<Integer, List<RightOpenInterval>> entry : dimensionsStrides.entrySet()) {
            final int currentDim = entry.getKey();
            final List<RightOpenInterval> currentDimIntervals = entry.getValue();
            final int nextDimension = currentDim + 1 ;
            final List<RightOpenInterval> nextDimensionIntervals = dimensionsStrides.get(nextDimension);
            if(!isNullOrEmpty(nextDimensionIntervals)) {
                for(final RightOpenInterval currentInterval : currentDimIntervals) {
                    for(final RightOpenInterval nextDimensionInterval : nextDimensionIntervals) {
                        id++;
                        //This String repsentation might turn out to be important later
                        cornerPointsList.add(new GridCornerPoints(String.valueOf(id), currentInterval, nextDimensionInterval));
                    }
                }
            }
        }

        cornerPointsList.forEach( cornerPoints -> log.debug("CornerPoint {}", cornerPoints));

        //Now that we have created the p^n grids corner points, time to create grids
        final Multimap<String, double[]> gridPointRepository =  ArrayListMultimap.create();

        for(final double[] dataPoint : dataSet) {
            for(final GridCornerPoints cornerPoints : cornerPointsList) {
                final RightOpenInterval xAxisInterval = cornerPoints.getxAxisCornerPoints();
                final RightOpenInterval yAxisInterval = cornerPoints.getyAxisCornerPoints();

                final double minX = xAxisInterval.getStart();
                final double maxX = xAxisInterval.getEnd();
                final double minY = yAxisInterval.getStart();
                final double maxY = yAxisInterval.getEnd();
                final double xOrdinate = dataPoint[0];
                final double yOrdinate = dataPoint[1];

                if(
                   (xOrdinate >= minX && xOrdinate < maxX) //Because it is RightSideOpen
                && (yOrdinate >= minY && yOrdinate < maxY)
                )
                {
                    log.debug("Point {} belongs to {}", Arrays.toString(dataPoint), cornerPoints);
                    gridPointRepository.put(cornerPoints.getGridId(), dataPoint);
                    break;
                }
            }
        }

        //THIS HAS TO CHANGE LATER
        final int totalGrids = (int) Math.pow(partitions, dimension);
        final List<Grid> grids = cornerPointsList.stream()
                                                 .map(it -> {
                                                     final List<double[]> points = (List<double[]>) gridPointRepository.get(it.getGridId());
                                                     final int gridSize = points.size();
                                                     GridLabel gridLabel = NOT_DENSE;
                                                     if(gridSize  > ( 2 *  (dataSetSize / totalGrids)) ) {
                                                         gridLabel = DENSE;
                                                     }
                                                     return new Grid(points, gridLabel, it.getGridId() , it);
                                                }).collect(Collectors.toList());

        return grids;
    }


    private static double strideSize(final double min, final double max, final int p) {
        return Precision.round((max - min )/ p, 2, BigDecimal.ROUND_HALF_DOWN);
    }


    public static void main(String[] args) throws Exception {

//        List<double[]> points = Arrays.asList(
//                new double[]{1, 2},
//                new double[]{0, 1} ,
//                new double[]{9, 8},
//                new double[]{11, 13},
//                new double[]{7, 9},
//                new double[]{6, 8}
//        );

        String fileLocation = "/Users/ankushsharma/Downloads/Factice_2Dexample.csv";
        List<double[]> dataSet = Files.readAllLines(Paths.get(fileLocation))
                .stream()
                .map(string -> string.split(",")) // Each line is a string, we break it based on delimiter ',' . This gives us an array
                .skip(1) //Skip the header
                .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])}) // The 2nd and 3rd column in the csv file
                .collect(Collectors.toList());



        ParallelDBScan parallelDBScan = new ParallelDBScan(
                dataSet, 10, 5, 8
        );

        parallelDBScan.compute();


//        List<double[]> points = Arrays.asList(
//                new double[]{1.1},
//                new double[]{2.1},
//                new double[]{4.1}
//        );
//        List<Grid> grids = splitIntoGrids(dataSet, 8);
//        for (Grid grid : grids) {
//            System.out.println(grid);
//        }
    }


}
