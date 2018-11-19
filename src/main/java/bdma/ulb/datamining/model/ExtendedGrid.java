package bdma.ulb.datamining.model;


public class ExtendedGrid {

    private final Grid grid;
    private final double epsilon;

    public ExtendedGrid(
            final Grid grid,
            final double epsilon
    )
    {
        this.grid = grid;
        this.epsilon = epsilon;
    }

    public Grid getGrid() {
        return grid;
    }

    public double getEpsilon() {
        return epsilon;
    }
}
