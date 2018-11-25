package bdma.ulb.datamining.algo;

import bdma.ulb.datamining.model.*;
import bdma.ulb.datamining.util.Assert;
import bdma.ulb.datamining.util.Util;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;

public class ParallelDBScan implements IDbScan {

    private static final Logger log = LoggerFactory.getLogger(ParallelDBScan.class);
    private final ExecutorService executor;

    private final List<double[]> dataSet;
    private final double epsilon;
    private final int minPts;
    private final int partitions;
    private final int workers;

    public ParallelDBScan(
            final List<double[]> dataSet,
            final double epsilon,
            final int minPts,
            final int partitions,
            final int workers
    )
    {
        Assert.notNull(dataSet, "dataSet cannot be null");
        Assert.isTrue(epsilon > ZERO, "epsilon must be a double value greater than 0");
        Assert.isTrue(minPts > ZERO, "minPts must be an integer greater than one");
        Assert.isTrue(partitions > ZERO, "minPts must be an integer greater than one");
        Assert.isTrue(workers > ZERO, "workers must be an integer greater than one");


        this.dataSet = dataSet;
        this.epsilon = epsilon;
        this.minPts = minPts;
        this.partitions = partitions;
        this.workers = workers;
        log.info("Initializing Task pool with {} worker threads", workers);
        this.executor = Executors.newFixedThreadPool(workers);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public List<Cluster> compute() throws Exception {
        log.info("Creating {} grids", Math.pow(partitions,2 ));
        // First, divide the dataset into "partitions" number of grids
        final List<Grid> grids = splitIntoGrids(dataSet, partitions);

        //Build a Cache of Grid points. Each key is a point in the dataset, and has the "Grid Id" as value associated with it
        log.info("Building Grid Cache");
        final Map<double[], String> gridCache = buildGridCache(grids);

        log.info("Building Adjacency List of neighbours");
        final Map<String, Set<Grid>> adjacencyListOfNeighbours = createAdjacencyListOfNeighbours(grids,epsilon);
        adjacencyListOfNeighbours.forEach((k, v) -> {
            log.info("Grid Id {} . Adjacency list ->", k, getGridIds(v));
        });


        final SetMultimap<String, Grid> candidateEpsNbdGridsMultiMap = buildExtendedGrids(grids, adjacencyListOfNeighbours, gridCache);

        final Map<String, Collection<Grid>> epsilonNbdGridsList = candidateEpsNbdGridsMultiMap.asMap();

        final List<ComplexGrid> denseComplexGrids = buildDenseComplexGrids(grids, epsilonNbdGridsList);
        log.info("Number of Dense Complex Grids formed  {}", denseComplexGrids.size());
        final List<ComplexGrid> nonDenseComplexGrids = grids.stream()
                                                            .filter(grid -> grid.getLabel() == NOT_DENSE)
                                                            .map(grid -> new ComplexGrid(singletonList(grid)))
                                                            .collect(toList());

        log.info("Number of Non Desne Complex Grids formed  {}", nonDenseComplexGrids.size());

        //We need List of all Complex Grids, be it dense or non dense
        final List<ComplexGrid> allComplexGrids = new ArrayList<>(nonDenseComplexGrids);
        allComplexGrids.addAll(denseComplexGrids);
        Util.printComplexGrids(allComplexGrids);

        //We perform parallel DB Scan
        log.info("Preparing to run parallel DB Scan");
        final List<Future<PartitionResult>> resultPoolFutures = performParallelDbScan(allComplexGrids);

        //Each DB Scan result is now available as a Java Future. Simply loop over all futures, and get the result from each worker thread
        final List<PartitionResult> parallelComputationResults = new ArrayList<>();
        for(final Future<PartitionResult> resultFuture : resultPoolFutures) {
            final PartitionResult pResult = resultFuture.get();
            parallelComputationResults.add(pResult);
        }
        log.info("Parallel DB Scan executed successfully");
        final List<Cluster> clusters = parallelComputationResults.stream()
                                                                 .map(PartitionResult::getClusters)
                                                                 .flatMap(Collection :: stream)
                                                                 .collect(toList());

        log.info("Attempting to Merge clusters. We have {} clusters to merge {}", clusters.size());
        final List<Cluster> mergedCluster = mergeClusters(clusters);
        log.info("Final Result : Number of Clusters {}", clusters.size());
        mergedCluster.forEach(cluster -> {
            log.info("Cluster Size {}", cluster.getSize());
        });

        Util.printClusters(mergedCluster);
        try {
            executor.shutdown();
        }
        catch(final Exception ex) {
            log.error("" ,ex);
        }
        return mergedCluster;
    }


    private static double strideSize(final double min, final double max, final int p) {
        return Precision.round((max - min )/ p, 2, BigDecimal.ROUND_HALF_DOWN);
    }


    private Map<double[], String> buildGridCache(final List<Grid> grids) {
        final Map<double[], String> gridCache = new HashMap<>();
        for(final Grid grid : grids) {
            for(final double[] point : grid.getDataPoints()) {
                gridCache.put(point, grid.getId());
            }
        }
        return gridCache;
    }

    private List<Future<PartitionResult>> performParallelDbScan(final List<ComplexGrid> complexGrids) {
        final List<Future<PartitionResult>> resultPoolFutures = new ArrayList<>();
        for(final ComplexGrid complexGrid : complexGrids) {
            final Future<PartitionResult> result = executor.submit( () -> {
                final List<double[]> dataSet = complexGrid.getAllPoints();
                final DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
                final List<Cluster> clusters = dbScan.compute();
                return new PartitionResult(clusters);
            });
            resultPoolFutures.add(result);
        }
        return resultPoolFutures;
    }

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

        //Now that we have created the p^n grids corner points, time to create grids
        final Multimap<String, double[]> gridPointRepository =  LinkedListMultimap.create();

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

        //Here is where we are supposed to split recursively, but we don't.
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


    private SetMultimap<String, Grid> buildExtendedGrids(
            final List<Grid> grids,
            final Map<String, Set<Grid>> adjacencyListOfNbrs,
            final Map<double[], String> gridCache)

    {
        final SetMultimap<String, Grid> result = HashMultimap.create();
        for(final Grid grid : grids) {
            final String gridId = grid.getId();
            final List<double[]> pointsFromAdjacentNeighbours = mergeGrids(adjacencyListOfNbrs.get(gridId));
            final List<double[]> points = grid.getDataPoints();
            final Map<double[], Boolean> visited = new HashMap<>();
            int index = 0;
            while(index < points.size() ) {
                final double[] current = points.get(index);
                index ++;
                if(isNull(visited.get(current))) {
                    visited.put(current, true);
                    final List<double[]> neighbours = getNeighbours(current, pointsFromAdjacentNeighbours, epsilon);
                    if (!isNullOrEmpty(neighbours)) {
                        for(final double[] neighbour : neighbours) {
                            final String neighbourPointGridId = gridCache.get(neighbour);
                            final Grid neighbourPointGrid = grids.stream().filter(g -> g.getId().equalsIgnoreCase(neighbourPointGridId)).findFirst().get();

                            if(!Objects.equals(grid, neighbourPointGrid)) {
                                grid.addExtendedPoints(neighbour);
                                result.put(gridId, neighbourPointGrid);
                            }
                        }
                    }
                }
            }
        }
        return result;
    }


    private List<ComplexGrid> buildDenseComplexGrids(final List<Grid> grids, final Map<String, Collection<Grid>> epsilonNbdGridsList) {
        final List<ComplexGrid> denseComplexGrids = new ArrayList<>();
        //Merge dense grids together
        for(final Grid grid : grids) {
            if(grid.isDense()) {
                final String gridId = grid.getId();
                Set<Grid> gridsInEpsilonNbd = (Set<Grid>) epsilonNbdGridsList.get(gridId);
                if(isNullOrEmpty(gridsInEpsilonNbd)) {
                    gridsInEpsilonNbd = Collections.emptySet();
                }
                // Any grid that is in under epsilon nbd of a grid and shares a point with a grid, and is also dense , is adjacent
                final Set<Grid> adjacentGrids = gridsInEpsilonNbd.stream()
                                                                 .filter(Grid ::isDense)
                                                                 .collect(Collectors.toSet());
                final List<Grid> complexGrids = new ArrayList<>(adjacentGrids);
                complexGrids.add(grid);
                denseComplexGrids.add(new ComplexGrid(complexGrids));
            }
        }
        return denseComplexGrids;
    }


    private List<Cluster> mergeClusters(final List<Cluster> clusters) {
        final Iterator<Cluster> iterator = clusters.iterator();
        while(iterator.hasNext()) {
            final Cluster currentCluster = iterator.next();
            final Iterator<Cluster> visitedIterator = clusters.iterator();
            while(visitedIterator.hasNext()) {
                final Cluster visitedCluster = visitedIterator.next();
                if(!Objects.equals(visitedCluster, currentCluster)) {
                    final Set<double[]> sharedPoints = currentCluster.getCommonPoints(visitedCluster);
                    boolean toMerge = false;
                    final Map<double[], List<double[]>> sharedPointEpsilonNbdMap = new HashMap<>();
                    List<double[]> mergedDataPoints;
                    if(!isNullOrEmpty(sharedPoints)) {
                        mergedDataPoints = merge(Arrays.asList(currentCluster, visitedCluster));
                        for(double[] sharedPoint : sharedPoints) {
                            final List<double[]> nbd = getNeighbours(sharedPoint, mergedDataPoints, epsilon);
                            sharedPointEpsilonNbdMap.put(sharedPoint, nbd);
                        }
                        for(final Entry<double[], List<double[]>> sharedPointNbd : sharedPointEpsilonNbdMap.entrySet()) {
                            final List<double[]> nbd = sharedPointNbd.getValue();
                            if(nbd.size() >= minPts) {
                                toMerge = true;
                                break;
                            }
                        }
                    }
                    if(toMerge) {
                        //Destroy visitedCluster from cluster
                        currentCluster.addDataPoints(visitedCluster.getDataPoints());
                        visitedIterator.remove();
                        return mergeClusters(clusters);
                    }
                    else {
                        for(double[] sharedPoint : sharedPoints) {
                            final List<double[]> currentClusterPoints = currentCluster.getDataPoints();
                            final List<double[]> visitedClusterPoints = visitedCluster.getDataPoints();
                            final List<double[]> currentClusterNbd = getNeighbours(sharedPoint, currentClusterPoints, epsilon);
                            final List<double[]> visitedClusterNbd = getNeighbours(sharedPoint, visitedClusterPoints, epsilon);
                            if(currentClusterNbd.size() > visitedClusterNbd.size()) {
                                visitedCluster.remove(sharedPoint);
                            }
                            else if(currentClusterNbd.size() == visitedClusterNbd.size()) {
                                final double distanceCurrentCluster = getNeighboursDistance(sharedPoint, currentClusterPoints);
                                final double distanceVisitedCluster = getNeighboursDistance(sharedPoint, visitedClusterPoints);
                                if(distanceCurrentCluster >= distanceVisitedCluster) {
                                    currentCluster.remove(sharedPoint);
                                }
                                else {
                                    visitedCluster.remove(sharedPoint);
                                }
                            }
                            else {
                                currentCluster.remove(sharedPoint);
                            }
                        }
                    }
                }
            }
        }
        return clusters;
    }

   private double getNeighboursDistance(final double[] point, final List<double[]> neighbours) {
        double addedDistance = ZERO;
        for(final double[] dataPoint : neighbours) {
            final double distance = computeEuclideanDistance(point, dataPoint);
            addedDistance += distance;
        }
        return addedDistance;
    }

    private static List<double[]> merge(List<Cluster> list) {
        return list
                .stream()
                .map(Cluster::getDataPoints)
                .flatMap(Collection :: stream)
                .distinct()
                .collect(toList());
    }

    private List<double[]> mergeGrids(final Set<Grid> grids) {
        return grids.stream().map(Grid::getDataPoints)
                            .flatMap(Collection :: stream)
                            .collect(Collectors.toList());
    }

    private static Map<String, Set<Grid>> createAdjacencyListOfNeighbours(final List<Grid> grids, final double epsilon) {
        final Map<String, Set<Grid>> adjacencyList = new HashMap<>();
        for(final Grid grid : grids) {
            final GridCornerPoints cornerPoints = grid.getCornerPoints();
            final RightOpenInterval xInterval = cornerPoints.getxAxisCornerPoints();
            final RightOpenInterval yInterval = cornerPoints.getyAxisCornerPoints();

            final double minX = xInterval.getStart();
            final double maxX = xInterval.getEnd();
            final double minY = yInterval.getStart();
            final double maxY = yInterval.getEnd();

            //Extend episloon in each direction
            final double extendedMinX = minX - epsilon;
            final double extendedMaxX = maxX + epsilon;
            final double extendedMinY = minY - epsilon;
            final double extendedMaxY = maxY + epsilon;
            final Set<Grid> adjacentGrids = new HashSet<>();
            for(final Grid otherGrid : grids) {
                if(!Objects.equals(grid, otherGrid)) {
                    final GridCornerPoints otherGridCornerPoints = otherGrid.getCornerPoints();
                    final RightOpenInterval xIntervalOtherGrid = otherGridCornerPoints.getxAxisCornerPoints();
                    final RightOpenInterval yIntervalOtherGrid = otherGridCornerPoints.getyAxisCornerPoints();

                    final double minXOtherGrid = xIntervalOtherGrid.getStart();
                    final double maxXOtherGrid = xIntervalOtherGrid.getEnd();
                    final double minYOtherGrid = yIntervalOtherGrid.getStart();
                    final double maxYOtherGrid = yIntervalOtherGrid.getEnd();

                    if(
                            (extendedMinX <= maxXOtherGrid && maxXOtherGrid <= minX && extendedMaxY >= minYOtherGrid && extendedMinY <= maxYOtherGrid) ||
                                    (extendedMaxY >= minYOtherGrid  && extendedMinY <= maxYOtherGrid && minX == minXOtherGrid && maxX == maxXOtherGrid) ||
                                    (maxX <= maxXOtherGrid  && extendedMaxX >= minXOtherGrid && extendedMaxY >= minYOtherGrid && extendedMinY <= maxYOtherGrid)
                            )
                    {
                        adjacentGrids.add(otherGrid);
                    }

                }
            }
            adjacencyList.putIfAbsent(grid.getId(), adjacentGrids);
        }
        return adjacencyList;
    }

    public Set<String> getGridIds(final Set<Grid> grids) {
        return grids.stream()
                    .map(Grid::getId)
                    .collect(Collectors.toSet());
    }



    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        String fileLocation = "/Users/ankushsharma/Desktop/code/dbscan/src/main/resources/Factice_2Dexample.csv";
        List<double[]> dataSet = Files.readAllLines(Paths.get(fileLocation))
                                      .stream()
                                      .map(string -> string.split(",")) // Each line is a string, we break it based on delimiter ',' . This gives us an array
                                        .skip(1) //Skip the header
                                        .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])}) // The 2nd and 3rd column in the csv file
                                        .collect(Collectors.toList());


        double epsilon = 10;
        int minPts = 5;
        int workers = Runtime.getRuntime().availableProcessors();

        ParallelDBScan parallelDBScan = new ParallelDBScan(
                dataSet, epsilon, minPts, 8, workers
        );
        List<Cluster> clusters = parallelDBScan.compute();

        for(Cluster cluster : clusters) {
            System.out.println(cluster.getSize());
        }

    }



}
