package io.github.melin.sqlflow.tree;

import io.github.melin.sqlflow.AstVisitor;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:02 AM
 */
public final class ProcessingMode
        extends Node {
    private final Mode mode;

    public ProcessingMode(NodeLocation location, Mode mode) {
        this(Optional.of(location), mode);
    }

    public ProcessingMode(Optional<NodeLocation> location, Mode mode) {
        super(location);
        this.mode = requireNonNull(mode, "mode is null");
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitProcessingMode(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        return mode == ((ProcessingMode) obj).mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("mode", mode)
                .toString();
    }

    public enum Mode {
        RUNNING, FINAL
    }
}
