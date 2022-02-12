package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.AstVisitor;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:34 PM
 */
public final class LikeClause extends TableElement {
    private final QualifiedName tableName;
    private final Optional<PropertiesOption> propertiesOption;

    public enum PropertiesOption {
        INCLUDING,
        EXCLUDING
    }

    public LikeClause(QualifiedName tableName, Optional<PropertiesOption> propertiesOption) {
        this(Optional.empty(), tableName, propertiesOption);
    }

    public LikeClause(NodeLocation location, QualifiedName tableName, Optional<PropertiesOption> propertiesOption) {
        this(Optional.of(location), tableName, propertiesOption);
    }

    private LikeClause(Optional<NodeLocation> location, QualifiedName tableName, Optional<PropertiesOption> propertiesOption) {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.propertiesOption = requireNonNull(propertiesOption, "propertiesOption is null");
    }

    public QualifiedName getTableName() {
        return tableName;
    }

    public Optional<PropertiesOption> getPropertiesOption() {
        return propertiesOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLikeClause(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LikeClause o = (LikeClause) obj;
        return Objects.equals(this.tableName, o.tableName) &&
                Objects.equals(this.propertiesOption, o.propertiesOption);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, propertiesOption);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("propertiesOption", propertiesOption)
                .toString();
    }
}
