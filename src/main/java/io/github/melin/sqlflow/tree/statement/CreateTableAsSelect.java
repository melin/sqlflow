package io.github.melin.sqlflow.tree.statement;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.tree.*;
import io.github.melin.sqlflow.tree.NodeLocation;
import io.github.melin.sqlflow.tree.QualifiedName;
import io.github.melin.sqlflow.tree.expression.Identifier;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.Property;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 12:45 PM
 */
public class CreateTableAsSelect
        extends Statement {
    private final QualifiedName name;
    private final Query query;
    private final boolean notExists;
    private final List<Property> properties;
    private final boolean withData;
    private final Optional<List<Identifier>> columnAliases;
    private final Optional<String> comment;

    public CreateTableAsSelect(QualifiedName name, Query query, boolean notExists, List<Property> properties, boolean withData, Optional<List<Identifier>> columnAliases, Optional<String> comment) {
        this(Optional.empty(), name, query, notExists, properties, withData, columnAliases, comment);
    }

    public CreateTableAsSelect(NodeLocation location, QualifiedName name, Query query, boolean notExists, List<Property> properties, boolean withData, Optional<List<Identifier>> columnAliases, Optional<String> comment) {
        this(Optional.of(location), name, query, notExists, properties, withData, columnAliases, comment);
    }

    private CreateTableAsSelect(Optional<NodeLocation> location, QualifiedName name, Query query, boolean notExists, List<Property> properties, boolean withData, Optional<List<Identifier>> columnAliases, Optional<String> comment) {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
        this.withData = withData;
        this.columnAliases = columnAliases;
        this.comment = requireNonNull(comment, "comment is null");
    }

    public QualifiedName getName() {
        return name;
    }

    public Query getQuery() {
        return query;
    }

    public boolean isNotExists() {
        return notExists;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public boolean isWithData() {
        return withData;
    }

    public Optional<List<Identifier>> getColumnAliases() {
        return columnAliases;
    }

    public Optional<String> getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableAsSelect(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.<Node>builder()
                .add(query)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, properties, withData, columnAliases, comment);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateTableAsSelect o = (CreateTableAsSelect) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && Objects.equals(notExists, o.notExists)
                && Objects.equals(properties, o.properties)
                && Objects.equals(withData, o.withData)
                && Objects.equals(columnAliases, o.columnAliases)
                && Objects.equals(comment, o.comment);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("withData", withData)
                .add("columnAliases", columnAliases)
                .add("comment", comment)
                .toString();
    }
}
