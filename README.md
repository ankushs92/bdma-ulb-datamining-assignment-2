** Usage **

DBScan requires 3 parameters :
1. epsilon : this is the radius
2. minPts : the minimum number of points required within the epsilon - nbd 
3. dataSet : the dataSet, passed in as a List<double[]>


First, put the file location in fileLocation variable.

   
```java

    public static void main(String[] args) throws IOException {
        String fileLocation = "/Users/ankushsharma/Downloads/Factice_2Dexample.csv";
        List<double[]> dataSet = Files.readAllLines(Paths.get(fileLocation))
                                      .stream()
                                      .map(string -> string.split(",")) // Each line is a string, we break it based on delimiter ',' . This gives us an array
                                      .skip(1) //Skip the header
                                      .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])}) // The 2nd and 3rd column in the csv file
                                      .collect(Collectors.toList());

        double epsilon = 10;
        int minPts = 5;

        DBScan dbScan = new DBScan(dataSet, epsilon, minPts);
        List<Cluster> clusters = dbScan.compute();

        long stop = System.currentTimeMillis();
        System.out.println("Time taken " + (stop-start) / 1000) ;
        for(Cluster cluster : clusters) {
            System.out.println(cluster.getSize());
        }


    }

    private static List<String> stringRepresentation(final List<double[]> points) {
        return points.stream()
                      .map(Arrays::toString)
                      .collect(Collectors.toList());
    }


```

Or, if you prefer, go to Test.java, and just run the code.


For ParallelDBScan : 

```java

        String fileLocation = "/Users/ankushsharma/Downloads/ex_Aggregation.csv";
        List<double[]> dataSet = Files.readAllLines(Paths.get(fileLocation))
                                      .stream()
                                      .map(string -> string.split(",")) // Each line is a string, we break it based on delimiter ',' . This gives us an array
                                      .skip(1) //Skip the header
                                      .map(array -> new double[]{Double.valueOf(array[1]), Double.valueOf(array[2])}) // The 2nd and 3rd column in the csv file
                                      .collect(Collectors.toList());

        double epsilon = 1.8;
        int minPts = 1550;
        int workers = Runtime.getRuntime().availableProcessors();
        long start = System.currentTimeMillis();

        ParallelDBScan dbScan = new ParallelDBScan(dataSet, epsilon, minPts, workers, Runtime.getRuntime().availableProcessors());
        List<Cluster> clusters = dbScan.compute();

        long stop = System.currentTimeMillis();
        System.out.println("Time taken " + (stop-start) / 1000) ;
        for(Cluster cluster : clusters) {
            System.out.println(cluster.getSize());
        }


```
