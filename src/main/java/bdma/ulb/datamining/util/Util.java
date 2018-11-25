package bdma.ulb.datamining.util;

import bdma.ulb.datamining.model.Cluster;
import bdma.ulb.datamining.model.ComplexGrid;
import bdma.ulb.datamining.model.Grid;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Util {

    public static List<String> stringRepresentation(final List<double[]> points) {
        return points.stream()
                .map(Arrays::toString)
                .collect(Collectors.toList());
    }

    public static boolean isNullOrEmpty(final Collection<?> collection) {
        if(Objects.isNull(collection) || collection.isEmpty()) {
            return true;
        }
        return false;
    }


     public static void printComplexGrids(final List<ComplexGrid> complexGrids) throws IOException {
        final File file = new File("src/main/resources/complex.csv");
        file.createNewFile();
        final String path = file.getPath();
        final CSVWriter writer = new CSVWriter(new FileWriter(path),
                                     CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        final List<String[]> headers = new ArrayList<>();
        headers.add(new String[]{"id", "complexID", "gridID", "label", "x", "y"});
        writer.writeAll(headers);
        int index = 0;
        for(final ComplexGrid complexGrid : complexGrids) {
            for(final Grid grid : complexGrid.getGrids()) {
                final List<String[]> output = new ArrayList<>();
                for(final double[] point : grid.getAllPoints()) {
                    output.add(new String[]{String.valueOf(index), complexGrid.getGridIds().stream().collect(Collectors.joining("_")), grid.getId(), grid.getLabel().name(), String.valueOf(point[0]), String.valueOf(point[1])});
                    index = index + 1;
                }
                writer.writeAll(output);
            }
        }
        writer.close();
    }

    public static Map<Integer, Cluster> printClusters(final List<List<Cluster>> clusters) throws IOException {
        File file = new File("src/main/resources/clusters.csv");
        file.createNewFile();
        final String path = file.getPath();
        final CSVWriter writer = new CSVWriter(new FileWriter(path),
                CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        final List<String[]> headers = new ArrayList<>();
        headers.add(new String[]{"id", "clusterId", "x", "y"});
        writer.writeAll(headers);
        Map<Integer, Cluster> clustersWithId = new HashMap<>();
        int index = 0;
        int complexIndex = 0;
        for(List<Cluster> cS : clusters) {
            final List<String[]> output = new ArrayList<>();
            for(Cluster c : cS) {
                clustersWithId.putIfAbsent(complexIndex, c);

                for(final double[] point : c.getDataPoints()) {
                    output.add(new String[]{String.valueOf(index), String.valueOf(complexIndex), String.valueOf(point[0]), String.valueOf(point[1])});
                    index = index + 1;
                }
                writer.writeAll(output);
                complexIndex++;
            }
        }

        writer.close();
        return clustersWithId;
    }






}
