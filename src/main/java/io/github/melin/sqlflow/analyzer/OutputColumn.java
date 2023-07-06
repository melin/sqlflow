package io.github.melin.sqlflow.analyzer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@Immutable
public final class OutputColumn {
    private final String column;
    private final Set<Analysis.SourceColumn> sourceColumns;

    @JsonCreator
    public OutputColumn(@JsonProperty("column") String column, @JsonProperty("sourceColumns") Set<Analysis.SourceColumn> sourceColumns) {
        this.column = requireNonNull(column, "column is null");
        this.sourceColumns = ImmutableSet.copyOf(requireNonNull(sourceColumns, "sourceColumns is null"));
    }

    @JsonProperty
    public String getColumn() {
        return column;
    }

    @JsonProperty
    public Set<Analysis.SourceColumn> getSourceColumns() {
        return sourceColumns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, sourceColumns);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OutputColumn entry = (OutputColumn) obj;
        return Objects.equals(column, entry.column) &&
                Objects.equals(sourceColumns, entry.sourceColumns);
    }
}
