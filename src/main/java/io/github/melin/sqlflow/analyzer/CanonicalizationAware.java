package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.expression.Identifier;

import java.util.OptionalInt;

import static io.github.melin.sqlflow.util.AstUtils.treeEqual;
import static io.github.melin.sqlflow.util.AstUtils.treeHash;
import static java.util.Objects.requireNonNull;

public class CanonicalizationAware<T extends Node> {
    private final T node;

    // Updates to this field are thread-safe despite benign data race due to:
    // 1. idempotent hash computation
    // 2. atomic updates to int fields per JMM
    private int hashCode;

    private CanonicalizationAware(T node) {
        this.node = requireNonNull(node, "node is null");
    }

    public static <T extends Node> CanonicalizationAware<T> canonicalizationAwareKey(T node) {
        return new CanonicalizationAware<T>(node);
    }

    public T getNode() {
        return node;
    }

    @Override
    public int hashCode() {
        int hash = hashCode;
        if (hash == 0) {
            hash = treeHash(node, CanonicalizationAware::canonicalizationAwareHash);
            if (hash == 0) {
                hash = 1;
            }

            hashCode = hash;
        }

        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CanonicalizationAware<T> other = (CanonicalizationAware<T>) o;
        return treeEqual(node, other.node, CanonicalizationAware::canonicalizationAwareComparison);
    }

    @Override
    public String toString() {
        return "CanonicalizationAware(" + node + ")";
    }

    public static Boolean canonicalizationAwareComparison(Node left, Node right) {
        if (left instanceof Identifier && right instanceof Identifier) {
            Identifier leftIdentifier = (Identifier) left;
            Identifier rightIdentifier = (Identifier) right;

            return leftIdentifier.getCanonicalValue().equals(rightIdentifier.getCanonicalValue());
        }

        return null;
    }

    public static OptionalInt canonicalizationAwareHash(Node node) {
        if (node instanceof Identifier) {
            return OptionalInt.of(((Identifier) node).getCanonicalValue().hashCode());
        } else if (node.getChildren().isEmpty()) {
            return OptionalInt.of(node.hashCode());
        }
        return OptionalInt.empty();
    }
}
