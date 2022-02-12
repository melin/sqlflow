package com.github.melin.sqlflow.tree.statement;

import com.github.melin.sqlflow.AstVisitor;
import com.github.melin.sqlflow.tree.*;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.NodeLocation;
import com.github.melin.sqlflow.tree.Property;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:48 PM
 */
public class CreateMaterializedView extends Statement {
    private final QualifiedName name;
    private final Query query;
    private final boolean replace;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<String> comment;

    public CreateMaterializedView(Optional<NodeLocation> location,
                                  QualifiedName name,
                                  Query query,
                                  boolean replace,
                                  boolean notExists,
                                  List<Property> properties,
                                  Optional<String> comment) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.replace = replace;
        this.notExists = notExists;
        this.properties = properties;
        this.comment = comment;
    }

    public QualifiedName getName() {
        return name;
    }

    public Query getQuery() {
        return query;
    }

    public boolean isReplace() {
        return replace;
    }

    public boolean isNotExists() {
        return notExists;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public Optional<String> getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterializedView(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, replace, notExists, properties, comment);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateMaterializedView o = (CreateMaterializedView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(replace, o.replace)
                && Objects.equals(notExists, o.notExists)
                && Objects.equals(properties, o.properties)
                && Objects.equals(comment, o.comment);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("replace", replace)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("comment", comment)
                .toString();
    }
}
