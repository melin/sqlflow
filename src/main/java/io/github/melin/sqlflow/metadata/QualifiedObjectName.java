package io.github.melin.sqlflow.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedObjectName {
    @JsonCreator
    public static QualifiedObjectName valueOf(String name) {
        requireNonNull(name, "name is null");

        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        if (ids.size() == 3) {
            return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
        } else {
            return new QualifiedObjectName(null, ids.get(0), ids.get(1));
        }
    }

    private final String catalogName;
    private final String schemaName;
    private final String objectName;

    public QualifiedObjectName(String catalogName, String schemaName, String objectName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.objectName = objectName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedObjectName o = (QualifiedObjectName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(objectName, o.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, objectName);
    }

    @JsonValue
    @Override
    public String toString() {
        if (catalogName != null) {
            return catalogName + '.' + schemaName + '.' + objectName;
        } else {
            return schemaName + '.' + objectName;
        }
    }
}
