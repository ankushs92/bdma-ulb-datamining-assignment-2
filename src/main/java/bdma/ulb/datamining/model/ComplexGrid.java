package bdma.ulb.datamining.model;

import java.util.List;
import java.util.stream.Collectors;

public class ComplexGrid {

    private final List<Grid> grids;

    public ComplexGrid(final List<Grid> grids) {
        this.grids = grids;
    }

    public List<Grid> getGrids() {
        return grids;
    }

    public List<String> getGridIds() {
        return grids.stream().map(Grid :: getId).distinct().collect(Collectors.toList());
    }
}
