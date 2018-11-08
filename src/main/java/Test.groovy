class Test {

    public static void main(String[] args) {
        def file = new File("/Users/ankushsharma/Downloads/Factice_2Dexample.csv")
        def data = file.readLines()
                        .subList(1, file.readLines().size() - 1)
                        .collect {
                        def array = it.split(",")
//                        def doublePoint = new DoublePoint([array[1], array[2]] as double[])
//                        doublePoint
                        [array[1], array[2]] as double[]
                    }
//        def library = new DBSCANClusterer(10, 5)
//        def librayResult = library.cluster(data)
//        librayResult.each {
//            println it.points.size()
//        }
//

        def s = new DBScan(data, 10, 5)
       def result =  s.compute()
        result.each {
            println it.size()
            println it
        }
    }
}
