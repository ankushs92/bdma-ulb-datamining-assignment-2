** Usage **

DBScan requires 3 parameters :
1. epsilon : this is the radius
2. minPts : the minimum number of points required within the epsilon - nbd 
3. dataSet : the dataSet, passed in as a List<double[]>


First, put the file location in fileLocation variable.

   
```java

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


```

Or, if you prefer, go to Test.java, and just run the code.
