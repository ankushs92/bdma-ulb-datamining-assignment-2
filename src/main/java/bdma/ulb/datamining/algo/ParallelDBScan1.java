package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.*;
import bdma.ulb.datamining.util.Assert;
import bdma.ulb.datamining.util.Util;
import com.google.common.collect.*;
import com.opencsv.CSVWriter;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.StringValueExp;
import javax.swing.plaf.synth.SynthTabbedPaneUI;
import java.io.FileWriter;
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
import static java.util.Objects.*;
import static java.util.stream.Collectors.toList;

public class ParallelDBScan1 implements IDbScan {

    private static final Logger log = LoggerFactory.getLogger(ParallelDBScan.class);
    private static final int cores = Runtime.getRuntime().availableProcessors();
    private static final int workers = 3;  //For now
    private static final ExecutorService executor = Executors.newFixedThreadPool(workers);

    private final List<double[]> dataSet;
    private final double epsilon;
    private final int minPts;
    private final int partitions;

    public ParallelDBScan1(
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
    @SuppressWarnings("Duplicates")
    public List<Cluster> compute() throws Exception {
        // First, divide the dataset into "partitions" number of grids
        final List<Grid> grids = splitIntoGrids(dataSet, partitions, epsilon);
        printToCsv(grids);

        System.out.println("Printing Original Grids");
        for(final Grid grid : grids) {
            System.out.println("--------");
            System.out.println("Grid Id " + grid.getId());
            System.out.println("Grid Dense or NOT : " + grid.getLabel());
            System.out.println("Original Size : " + grid.getCount());

            System.out.println("--------");

        }

        final SetMultimap<String, Grid> candidateEpsNbdGridsMultiMap = HashMultimap.create();
        for(final Grid grid : grids) {
            final String gridId = grid.getId();
            final List<double[]> points = grid.getDataPoints();
            final Map<double[], Boolean> visited = new HashMap<>();
            int index = 0;
            while(index < points.size() ) {
                final double[] current = points.get(index);
                index ++;
                if(isNull(visited.get(current))) {
                    //System.out.println("visiting " + index + " which is " + current);
                    visited.put(current, true);
                    final List<double[]> neighbours = getNeighbours(current, dataSet, epsilon);
                    if (!isNullOrEmpty(neighbours)) {

                        for(final double[] neighbour : neighbours) {
                            final Grid neighbourPointGrid = grids
                                    .stream()
                                    .filter( it -> it.getDataPoints().stream().anyMatch(i -> Arrays.equals(neighbour, i)))
                                    .findFirst()
                                    .get();

                            if(!Objects.equals(grid, neighbourPointGrid)) {
                                grid.addExtendedPoints(neighbour);
                                candidateEpsNbdGridsMultiMap.put(gridId, neighbourPointGrid);
                            }
                        }
                    }
                }
            }
        }
        System.out.println("------------------------------------------------");

        System.out.println("Printing GRIDS AFTER ADDING points ");
        for(final Grid grid : grids) {
            System.out.println("--------");
            System.out.println("Grid Id " + grid.getId());
            System.out.println("Grid Dense or NOT : " + grid.getLabel());
            System.out.println("Actual Size : " + grid.getCount());

            System.out.println("--------");
        }
        System.out.println("------------------------------------------------");


        final Map<String, Collection<Grid>> epsilonNbdGridsList = candidateEpsNbdGridsMultiMap.asMap();
        final List<ComplexGrid> denseComplexGrids = new ArrayList<>();

        //Merge dense grids together
        for(final Grid grid : grids) {
            if(grid.isDense()) {
                final String gridId = grid.getId();

                Set<Grid> gridsInEpsilonNbd = (Set<Grid>) epsilonNbdGridsList.get(gridId);
                if(isNullOrEmpty(gridsInEpsilonNbd)) {
                    gridsInEpsilonNbd = Collections.emptySet();
                }
                final Set<Grid> adjacentGrids = gridsInEpsilonNbd.stream() // Any grid that is in under epsilon nbd of a grid and shares a point with a grid, and is also dense , is adjacent
                        .filter(Grid ::isDense)
                        .collect(Collectors.toSet());
                final List<Grid> complexGrids = new ArrayList<>(adjacentGrids);
                complexGrids.add(grid);
                denseComplexGrids.add(new ComplexGrid(complexGrids));

            }
        }

        System.out.println("--PRINTING EPSILON NBD GRIDS---");
        epsilonNbdGridsList.forEach((k,v) -> {
            System.out.println(k + "----> " + v.stream().map(grid -> grid.getId()).collect(toList()) );
        });


        System.out.println("--Printing CompleX Grids---");
        denseComplexGrids.forEach(c -> {
            System.out.println("Complex Formed with Grid with ids " + c.getGridIds() + "with size " + denseComplexGrids.size());
        });


        final List<ComplexGrid> nonDenseComplexGrids = grids.stream()
                .filter(grid -> grid.getLabel() == NOT_DENSE)
                .map(grid -> new ComplexGrid(singletonList(grid)))
                .collect(toList());


        //Multi threaded DBScan
        final List<ComplexGrid> allComplexGrids = new ArrayList<>(nonDenseComplexGrids);
        allComplexGrids.addAll(denseComplexGrids);
        printComplexGrids(allComplexGrids);

        final List<Future<ParallelComputationResult>> resultPool = new ArrayList<>();
        for(final ComplexGrid complexGrid : allComplexGrids) {
            final Future<ParallelComputationResult> result = executor.submit( () -> {
                final List<double[]> dataSet = complexGrid.getAllPoints();
                final DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
                final List<Cluster> clusters = dbScan.compute();
                return new ParallelComputationResult(complexGrid, clusters);
            });
            resultPool.add(result);
        }

        //Merging step
        List<ParallelComputationResult> parallelComputationResults = new ArrayList<>();
        for(final Future<ParallelComputationResult> result : resultPool) {
            final ParallelComputationResult pResult = result.get();
            parallelComputationResults.add(pResult);
        }

        parallelComputationResults = parallelComputationResults.stream().filter( result -> result.getClusters().size() >= 1).collect(toList());


        parallelComputationResults.forEach(c -> {
            System.out.println( c.getComplexGrid().getGridIds() + " -------->  ");
            for(Cluster cluster : c.getClusters()) {
                System.out.println(cluster.getSize());
                System.out.println(Util.stringRepresentation(cluster.getDataPoints()));
            }
            System.out.println("-0-0-0-0-0-0-0-0-0-0-0");
        });
        printClusters(parallelComputationResults.stream().map(p->p.getClusters()).collect(toList()));


        try {
            executor.shutdown();
        }
        catch(final Exception ex) {
            log.error("" ,ex);
        }
        return null;
    }

    private static final double DELTA = 0.01;
    @SuppressWarnings("Duplicates")
    private static List<Grid> splitIntoGrids(final List<double[]> dataSet, final int partitions, final double epsilon) {
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
                    final List<double[]> initialPoints = (List<double[]>) gridPointRepository.get(it.getGridId());
                    GridLabel gridLabel = NOT_DENSE;
                    //We now extend the grid to epsilon in every direction
                    final int gridSize = initialPoints.size();
                    if(gridSize  >= ( 2 *  (dataSetSize / totalGrids)) ) {
                        gridLabel = DENSE;
                    }
                    return new Grid(initialPoints, gridLabel, it.getGridId() , it);
                }).collect(Collectors.toList());

        return grids;
    }


    private static double strideSize(final double min, final double max, final int p) {
        return Precision.round((max - min )/ p, 2, BigDecimal.ROUND_HALF_DOWN);
    }


//    private static Multimap<String, Grid> createAdjacencyListOfNeighbours(final List<Grid> grids) {
//        //Create adjacency list of neighbours
//        final Multimap<String, Grid> result = ArrayListMultimap.create();
//        for(final Grid grid : grids) {
//            final GridCornerPoints gridCornerPoints = grid.getCornerPoints();
//            for(final Grid otherGrid : grids) {
//                if(!Objects.equals(grid, otherGrid)) {
//                    final GridCornerPoints otherGridCornerPoints = otherGrid.getCornerPoints();
//                    if(gridCornerPoints.hasAnyBoundaryPointCommon(otherGridCornerPoints)) {
//                        result.put(grid.getId(), otherGrid);
//                    }
//                }
//            }
//        }
//        return result;
//    }


    private static Cluster mergeClusters(final List<Cluster> clusters) {
        List<double[]> distinctPoints = new ArrayList<>();
        for(final Cluster cluster : clusters) {
            final List<double[]> points = cluster.getDataPoints();
            distinctPoints.addAll(points);
        }
        distinctPoints = distinctPoints.stream().distinct().collect(toList());
        return new Cluster(distinctPoints);
    }

    @SuppressWarnings("Duplicates")
    private  List<ExtendedGrid> buildExtendedGrids(final List<Grid> grids, final double epsilon) {
        final List<ExtendedGrid> extendedGrids = grids.stream()
                .map(grid -> {
                    final List<double[]> gridPoints = grid.getDataPoints();
                    final GridCornerPoints cornerPoints = grid.getCornerPoints();
                    final RightOpenInterval xAxisInterval = cornerPoints.getxAxisCornerPoints();
                    final RightOpenInterval yAxisInterval = cornerPoints.getyAxisCornerPoints();
                    final double extendedMinX  = xAxisInterval.getStart() - epsilon;
                    final double extendedMaxX  = xAxisInterval.getEnd() + epsilon;
                    final double extendedMinY = yAxisInterval.getStart() - epsilon;
                    final double extendedMaxY  = yAxisInterval.getEnd() + epsilon;

                    for(final double[] point : dataSet) {
                        final double xOrdinate = point[0];
                        final double yOrdinate = point[1];
                        if( (xOrdinate >= extendedMinX && xOrdinate < extendedMaxX)
                                && (yOrdinate >= extendedMinY && yOrdinate < extendedMaxY)
                                )
                        {
                            gridPoints.add(point);
                        }
                    }
                    return new ExtendedGrid(grid, epsilon);
                })
                .collect(toList());
        return extendedGrids;
    }

    private static void printToCsv(final List<Grid> grids) throws IOException {
        final CSVWriter writer = new CSVWriter(new FileWriter("/Users/ankushsharma/bdma-ulb-datamining-dbscan/src/main/resources/test.csv"),
                CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        final List<String[]> headers = new ArrayList<>();
        headers.add(new String[]{"id", "gridID", "label", "x", "y"});
        writer.writeAll(headers);
        int index = 0;
        for(final Grid grid : grids) {
            final List<String[]> output = new ArrayList<>();
            for(final double[] point : grid.getDataPoints()) {
                output.add(new String[]{String.valueOf(index), grid.getId(), grid.getLabel().name(), String.valueOf(point[0]), String.valueOf(point[1])});
                index++;
            }
            writer.writeAll(output);
        }
        writer.close();
    }


    private static void printComplexGrids(final List<ComplexGrid> complexGrids) throws IOException {
        final CSVWriter writer = new CSVWriter(new FileWriter("/Users/ankushsharma/bdma-ulb-datamining-dbscan/src/main/resources/complex.csv"),
                CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        final List<String[]> headers = new ArrayList<>();
        headers.add(new String[]{"id", "complexID", "gridID", "label", "x", "y"});
        System.out.println("HEEEEEYYYYY " + complexGrids.size());
        writer.writeAll(headers);
        int index = 0;
        int complexIndex = 0;
        for(ComplexGrid c : complexGrids) {
            System.out.println("HEEEEEYYYYY Complex Formed with Grid with ids " + c.getGridIds());
            for(final Grid grid : c.getGrids()) {

                final List<String[]> output = new ArrayList<>();
                for(final double[] point : grid.getDataPoints()) {
                    output.add(new String[]{String.valueOf(index), c.getGridIds().stream().collect(Collectors.joining("_")), grid.getId(), grid.getLabel().name(), String.valueOf(point[0]), String.valueOf(point[1])});
                    index = index + 1;
                }
                writer.writeAll(output);
            }

        }

        writer.close();
    }

    private static void printClusters(final List<List<Cluster>> clusters) throws IOException {
        final CSVWriter writer = new CSVWriter(new FileWriter("/Users/ankushsharma/bdma-ulb-datamining-dbscan/src/main/resources/clusters.csv"),
                CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        final List<String[]> headers = new ArrayList<>();
        headers.add(new String[]{"id", "clusterId", "x", "y"});
        System.out.println("HEEEEEYYYYY " + clusters.size());
        writer.writeAll(headers);
        int index = 0;
        int complexIndex = 0;
        for(List<Cluster> cS : clusters) {
            final List<String[]> output = new ArrayList<>();
            for(Cluster c : cS) {
                //System.out.println("HEEEEEYYYYY Complex Formed with Grid with ids " + c.());

                for(final double[] point : c.getDataPoints()) {
                    output.add(new String[]{String.valueOf(index), String.valueOf(complexIndex), String.valueOf(point[0]), String.valueOf(point[1])});
                    index = index + 1;

                }
                writer.writeAll(output);
                complexIndex++;
            }

        }

        writer.close();
    }

    @SuppressWarnings("Duplicates")
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


        double epsilon = 10;
        int minPts = 5;
//        DBScan dbScan = new DBScan(dataSet, epsilon, minPts);

        ParallelDBScan parallelDBScan = new ParallelDBScan(
                dataSet, epsilon, minPts, 8
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
