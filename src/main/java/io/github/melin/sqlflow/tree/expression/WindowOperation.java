package io.github.melin.sqlflow.tree.expression;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.window.Window;
import io.github.melin.sqlflow.tree.window.WindowReference;
import io.github.melin.sqlflow.tree.window.WindowSpecification;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:04 AM
 */
public class WindowOperation
        extends Expression {
    private final Identifier name;
    private final Window window;

    public WindowOperation(Identifier name, Window window) {
        this(Optional.empty(), name, window);
    }

    public WindowOperation(NodeLocation location, Identifier name, Window window) {
        this(Optional.of(location), name, window);
    }

    private WindowOperation(Optional<NodeLocation> location, Identifier name, Window window) {
        super(location);
        requireNonNull(name, "name is null");
        requireNonNull(window, "window is null");
        checkArgument(window instanceof WindowReference || window instanceof WindowSpecification, "unexpected window: " + window.getClass().getSimpleName());

        this.name = name;
        this.window = window;
    }

    public Identifier getName() {
        return name;
    }

    public Window getWindow() {
        return window;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWindowOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(name, (Node) window);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowOperation o = (WindowOperation) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(window, o.window);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, window);
    }

    @Override
    public boolean shallowEquals(Node other) {
        return Node.sameClass(this, other);
    }
}
