package bdma.ulb.datamining

import bdma.ulb.datamining.algo.DBScan
import bdma.ulb.datamining.model.Cluster
import bdma.ulb.datamining.util.Util
import org.apache.commons.math3.ml.clustering.DBSCANClusterer
import org.apache.commons.math3.ml.clustering.DoublePoint

class Test1 {

    public static void main(String[] args) {
        double epsilon = 10
        int minPts = 5

        def file = new File("/Users/ankushsharma/Downloads/Factice_2Dexample.csv")
        def data = file.readLines()
                .subList(1, file.readLines().size() - 1)
                .collect {
            def array = it.split(",")
                        def doublePoint = new DoublePoint([array[1], array[2]] as double[])
                        doublePoint
//            [array[1], array[2]] as double[]
        }
        def library = new DBSCANClusterer(epsilon, minPts)
        def librayResult = library.cluster(data)
        librayResult.each {
            println it.points.size()
        }
//

//        def s = new DBScan(data, epsilon, minPts)
//        def result =  s.compute()
//        for(Cluster cluster in  result) {
//            //This had to be done because the default toString representation of double[] is just the hash code
//            System.out.println(Util.stringRepresentation(cluster.getDataPoints()));
//            System.out.println(cluster.getSize());
//        }
    }
}
