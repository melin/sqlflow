package com.github.melin.sqlflow.metadata;

import com.github.melin.sqlflow.SqlFlowException;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.analyzer.SemanticExceptions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/24 11:28 AM
 */
public class MetadataUtil {

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName) {
        checkCatalogName(catalogName);
        schemaName.ifPresent(name -> checkLowerCase(name, "schemaName"));
        tableName.ifPresent(name -> checkLowerCase(name, "tableName"));

        checkArgument(schemaName.isPresent() || tableName == null, "tableName specified but schemaName is missing");
    }

    public static String checkCatalogName(String catalogName) {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName) {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName) {
        return checkLowerCase(tableName, "tableName");
    }

    public static void checkObjectName(String catalogName, String schemaName, String objectName) {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(objectName, "objectName");
    }

    public static String checkLowerCase(String value, String name) {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
        return value;
    }

    public static QualifiedObjectName createQualifiedObjectName(MetadataService metadataService, Node node, QualifiedName name) {
        requireNonNull(metadataService, "metadata is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new SqlFlowException(format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : metadataService.getSchema().orElseThrow(() ->
                SemanticExceptions.semanticException(node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : metadataService.getCatalog().orElseThrow(() ->
                SemanticExceptions.semanticException(node, "Catalog must be specified when session catalog is not set"));

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
