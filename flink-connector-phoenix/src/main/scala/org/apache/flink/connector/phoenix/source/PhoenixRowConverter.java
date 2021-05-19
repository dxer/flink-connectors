package org.apache.flink.connector.phoenix.source;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class PhoenixRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public PhoenixRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Phoenix";
    }
}
