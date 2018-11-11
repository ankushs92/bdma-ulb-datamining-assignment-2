package bdma.ulb.datamining.model;

import java.util.List;

public class ComplexGrid {

    private final List<Grid> grids;

    public ComplexGrid(final List<Grid> grids) {
        this.grids = grids;
    }


    public List<Grid> getGrids() {
        return grids;
    }
}
