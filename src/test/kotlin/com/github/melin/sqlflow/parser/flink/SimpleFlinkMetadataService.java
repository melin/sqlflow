package com.github.melin.sqlflow.parser.flink;

import com.github.melin.sqlflow.metadata.*;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.github.melin.sqlflow.type.BigintType;
import com.github.melin.sqlflow.type.VarcharType;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleFlinkMetadataService implements MetadataService {
    @Override
    public Optional<String> getSchema() {
        return Optional.of("bigdata");
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.of("default");
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        if (table.getObjectName().equals("retek_xx_item_attr_translate_product_enrichment")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("ITEM", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("UDA_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("UDA_VALUE_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LANG", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("TRANSLATED_VALUE", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LAST_UPDATE_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("CREATE_DATETIME", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LAST_UPDATE_DATETIME", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("KAFKA_PROCESS_TIME", VarcharType.VARCHAR, false));

            return Optional.of(new SchemaTable("retek_xx_item_attr_translate_product_enrichment", columns));
        } else if (table.getObjectName().equals("mdm_dim_lang_lookupmap_oracle")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("LANG", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("ISO_CODE", BigintType.BIGINT, false));

            return Optional.of(new SchemaTable("mdm_dim_lang_lookupmap_oracle", columns));
        } else if (table.getObjectName().equals("mdm_dim_uda_item_ff_lookupmap_oracle")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("ITEM", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("UDA_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("UDA_TEXT", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LAST_UPDATE_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("CREATE_DATETIME", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LAST_UPDATE_DATETIME", BigintType.BIGINT, false));

            return Optional.of(new SchemaTable("mdm_dim_lang_lookupmap_oracle", columns));
        } else if (table.getObjectName().equals("mdm_dim_product_attrib_type_lookupmap_mysql")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("BU_CODE", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("ATTRIB_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("ATTRIB_TYPE", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("CONTROL_TYPE", BigintType.BIGINT, false));
            return Optional.of(new SchemaTable("mdm_dim_product_attrib_type_lookupmap_mysql", columns));
        } else if (table.getObjectName().equals("retek_uda_item_ff_product_enrichment_dim")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("ITEM", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("UDA_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("UDA_TEXT", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LAST_UPDATE_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("CREATE_DATETIME", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("LAST_UPDATE_DATETIME", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("KAFKA_PROCESS_TIME", BigintType.BIGINT, false));
            return Optional.of(new SchemaTable("retek_uda_item_ff_product_enrichment_dim", columns));
        } else if (table.getObjectName().equals("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("ITEM", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("UDA_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("UDA_VALUE_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("LANG", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("TRANSLATED_VALUE", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("LAST_UPDATE_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("CREATE_DATETIME", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("LAST_UPDATE_DATETIME", VarcharType.VARCHAR, false));
            return Optional.of(new SchemaTable("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim", columns));
        } else if (table.getObjectName().equals("processed_mdm_product_enrichment")) {
            List<ColumnSchema> columns = Lists.newArrayList();
            columns.add(new ColumnSchema("PRODUCT_ID", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("ENRICHMENT_ID", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("LANG", VarcharType.VARCHAR, false));
            columns.add(new ColumnSchema("ENRICHMENT_VALUE", BigintType.BIGINT, false));
            columns.add(new ColumnSchema("LAST_UPDATED", BigintType.BIGINT, false));
            return Optional.of(new SchemaTable("processed_mdm_product_enrichment", columns));
        }

        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
