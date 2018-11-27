package bdma.ulb.datamining;

import bdma.ulb.datamining.algo.DBScan;
import bdma.ulb.datamining.model.Cluster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleDbScanTest {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws IOException {
        File file = new File("src/main/resources/ex_Aggregation.csv");
        List<double[]> dataSet = Files.readAllLines(Paths.get(file.getPath()))
                .stream()
                .map(string -> string.split(",")) // Each line is a string, we break it based on delimiter ',' . This gives us an array
                .skip(1) //Skip the header
                .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])}) // The 2nd and 3rd column in the csv file
                .collect(Collectors.toList());

        double epsilon = 1.8;
        int minPts = 1550;
        long start = System.currentTimeMillis();

        DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
        List<Cluster> clusters = dbScan.compute();

        long stop = System.currentTimeMillis();
        System.out.println("Time taken " + (stop-start) / 1000) ;

    }

}
