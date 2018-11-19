package bdma.ulb.datamining.model;

import java.util.List;

public class ParallelComputationResult {

    private final ComplexGrid complexGrid;
    private final List<Cluster> clusters;

    public ParallelComputationResult(
            final ComplexGrid complexGrid,
            final List<Cluster> clusters
    )
    {
        this.complexGrid = complexGrid;
        this.clusters = clusters;
    }

    public ComplexGrid getComplexGrid() {
        return complexGrid;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }
}
