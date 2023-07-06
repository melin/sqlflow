package io.github.melin.sqlflow.tree;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * huaixin 2021/12/18 9:52 PM
 */
public final class NodeLocation {
    private final int line;
    private final int column;
    private final int startIndex;
    private final int stopIndex;

    public NodeLocation(int line, int column, int startIndex, int stopIndex) {
        checkArgument(line >= 1, "line must be at least one, got: %s", line);
        checkArgument(column >= 1, "column must be at least one, got: %s", column);

        this.line = line;
        this.column = column;
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return column;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getStopIndex() {
        return stopIndex;
    }

    @Override
    public String toString() {
        return "(" +
                "line=" + line +
                ", column=" + column +
                ", startIndex=" + startIndex +
                ", stopIndex=" + stopIndex +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeLocation that = (NodeLocation) o;
        return line == that.line &&
                column == that.column &&
                startIndex == that.startIndex &&
                stopIndex == that.stopIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(line, column, startIndex, stopIndex);
    }
}
