package bdma.ulb.datamining.model;

import java.util.List;

public class PartitionResult {

    private final List<Cluster> clusters;

    public PartitionResult(final List<Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }
}
