package com.github.melin.sqlflow.parser.flink;

import com.github.melin.sqlflow.metadata.*;
import com.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleFlinkMetadataService implements MetadataService {
    @Override
    public Optional<String> getSchema() {
        return Optional.of("default");
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.empty();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        if (table.getObjectName().equals("retek_xx_item_attr_translate_product_enrichment")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_VALUE_ID");
            columns.add("LANG");
            columns.add("TRANSLATED_VALUE");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            columns.add("KAFKA_PROCESS_TIME");

            return Optional.of(new SchemaTable("retek_xx_item_attr_translate_product_enrichment", columns));
        } else if (table.getObjectName().equals("mdm_dim_lang_lookupmap_oracle")) {
            List<String> columns = Lists.newArrayList();
            columns.add("LANG");
            columns.add("ISO_CODE");

            return Optional.of(new SchemaTable("mdm_dim_lang_lookupmap_oracle", columns));
        } else if (table.getObjectName().equals("mdm_dim_uda_item_ff_lookupmap_oracle")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_TEXT");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");

            return Optional.of(new SchemaTable("mdm_dim_lang_lookupmap_oracle", columns));
        } else if (table.getObjectName().equals("mdm_dim_product_attrib_type_lookupmap_mysql")) {
            List<String> columns = Lists.newArrayList();
            columns.add("BU_CODE");
            columns.add("ATTRIB_ID");
            columns.add("ATTRIB_TYPE");
            columns.add("CONTROL_TYPE");
            return Optional.of(new SchemaTable("mdm_dim_product_attrib_type_lookupmap_mysql", columns));
        } else if (table.getObjectName().equals("retek_uda_item_ff_product_enrichment_dim")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_TEXT");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            columns.add("KAFKA_PROCESS_TIME");
            return Optional.of(new SchemaTable("retek_uda_item_ff_product_enrichment_dim", columns));
        } else if (table.getObjectName().equals("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_VALUE_ID");
            columns.add("LANG");
            columns.add("TRANSLATED_VALUE");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            return Optional.of(new SchemaTable("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim", columns));
        } else if (table.getObjectName().equals("processed_mdm_product_enrichment")) {
            List<String> columns = Lists.newArrayList();
            columns.add("PRODUCT_ID");
            columns.add("ENRICHMENT_ID");
            columns.add("LANG");
            columns.add("ENRICHMENT_VALUE");
            columns.add("LAST_UPDATED");
            return Optional.of(new SchemaTable("processed_mdm_product_enrichment", columns));
        }

        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
