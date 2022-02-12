package com.github.melin.sqlflow.tree.expression;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 11:42 AM
 */
public class LabelDereference extends Expression {
    private final String label;
    private final Optional<SymbolReference> reference;

    public LabelDereference(String label, SymbolReference reference) {
        this(label, Optional.of(requireNonNull(reference, "reference is null")));
    }

    public LabelDereference(String label) {
        this(label, Optional.empty());
    }

    public LabelDereference(String label, Optional<SymbolReference> reference) {
        super(Optional.empty());
        this.label = requireNonNull(label, "label is null");
        this.reference = requireNonNull(reference, "reference is null");
    }

    public String getLabel() {
        return label;
    }

    public Optional<SymbolReference> getReference() {
        return reference;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLabelDereference(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return reference.<List<Node>>map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LabelDereference that = (LabelDereference) o;
        return Objects.equals(label, that.label) &&
                Objects.equals(reference, that.reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, reference);
    }

    @Override
    public boolean shallowEquals(Node other) {
        if (!sameClass(this, other)) {
            return false;
        }

        return label.equals(((LabelDereference) other).label);
    }
}
