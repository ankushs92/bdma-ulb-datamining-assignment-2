package bdma.ulb.datamining;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Test {

    public static void main(String[] args) throws IOException {
        String fileLocation = "/Users/ankushsharma/Downloads/Factice_2Dexample.csv";
        List<double[]> dataSet = Files.readAllLines(Paths.get(fileLocation))
                                      .stream()
                                      .map(string -> string.split(","))
                                      .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])})
                                      .collect(Collectors.toList());

        double epsilon = 10;
        int minPts = 5;

        DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
        List<List<double[]>> clusters = dbScan.compute();

        for(List<double[]> cluster : clusters) {
            System.out.println(cluster);
        }

    }

}
