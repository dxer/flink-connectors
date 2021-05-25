package org.apache.flink.connector.phoenix.sink;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.phoenix.Constans;
import org.apache.flink.connector.phoenix.DataSource;
import org.apache.flink.connector.phoenix.PhoenixOptions;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class PhoenixSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSinkFunction.class);

    public static final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;

    private PhoenixOptions phoenixOptions;

    private String[] fieldNames;

    private DataType[] fieldDataTypes;

    private int[] keyIndices;

    private int maxRetryTimes = 3;

    private HikariDataSource hikariDataSource;

    private String upsertSQL = null;
    private String delSQL = null;


    public PhoenixSinkFunction(PhoenixOptions phoenixOptions, String[] fieldNames, DataType[] fieldDataTypes, int[] keyIndices) {
        this.phoenixOptions = phoenixOptions;
        this.fieldNames = fieldNames;
        this.fieldDataTypes = fieldDataTypes;
        this.keyIndices = keyIndices;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("start open ...");
        super.open(parameters);
        String dbUrl = String.format(Constans.PHOENIX_JDBC_URL_TEMPLATE, phoenixOptions.getServerUrl());
        LOG.info("Phoenix JDBC URL: {}", dbUrl);

        upsertSQL = buildUpsetSQL(phoenixOptions, fieldNames);
        LOG.info("Upsert SQL: {}", upsertSQL);

        delSQL = buildDelSQL(phoenixOptions, fieldNames, keyIndices);
        LOG.info("Delete SQL: {}", delSQL);

        hikariDataSource = DataSource.newHikariDataSource(dbUrl, null, null);
        LOG.info("end open.");
    }

    /**
     * 生成 upsert sql
     *
     * @param options
     * @param fieldNames
     * @return
     */
    private String buildUpsetSQL(PhoenixOptions options, String[] fieldNames) {
        StringBuilder filedBuffer = new StringBuilder();
        StringBuilder valueBuffer = new StringBuilder();

        for (String field : fieldNames) {
            if (filedBuffer.toString().length() > 0) {
                filedBuffer.append(",");
            }
            filedBuffer.append(field);

            if (valueBuffer.toString().length() > 0) {
                valueBuffer.append(",");
            }
            valueBuffer.append("?");
        }

        StringBuilder builder = new StringBuilder();
        builder.append("UPSERT INTO ")
                .append(options.getTableName())
                .append(" (")
                .append(filedBuffer.toString())
                .append(") VALUES (")
                .append(valueBuffer.toString())
                .append(")");

        return builder.toString();
    }

    /**
     * 生成delete sql
     *
     * @param options
     * @param fieldNames
     * @param keyIndices
     * @return
     */
    private String buildDelSQL(PhoenixOptions options, String[] fieldNames, int[] keyIndices) {
        StringBuilder builder = new StringBuilder();

        builder.append("DELETE FROM ")
                .append(options.getTableName())
                .append(" WHERE ");
        for (int i = 0; i < keyIndices.length; i++) {
            if (i > 0) builder.append(" AND ");
            builder.append(fieldNames[i] + "=? ");
        }

        return builder.toString();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind rowKind = value.getRowKind();
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            Connection connection = hikariDataSource.getConnection();
            PreparedStatement pstmt = null;
            try {
                if (RowKind.INSERT == rowKind || RowKind.UPDATE_BEFORE == rowKind || RowKind.UPDATE_AFTER == rowKind) {
                    pstmt = connection.prepareStatement(upsertSQL);
                    setPstmtParams(value, pstmt, false); // 填充数据
                } else if (RowKind.DELETE == rowKind) {
                    pstmt = connection.prepareStatement(delSQL);
                    setPstmtParams(value, pstmt, true); // 填充数据
                }
                if (pstmt != null) pstmt.executeUpdate();
                break;
            } catch (Exception e) {
                LOG.error(String.format("JDBC execute error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            } finally {
                if (pstmt != null) pstmt.close();
                if (connection != null) connection.close();
            }
        }
    }

    /**
     * @param rowData
     * @param KeyFilter
     * @return
     */
    private List<Object> getValues(RowData rowData, boolean KeyFilter) {
        IntStream stream = null;
        if (KeyFilter) { // 过滤主键
            List<Integer> keyList = IntStream.range(0, keyIndices.length).mapToObj(x -> x).collect(Collectors.toList());
            stream = IntStream.range(0, fieldDataTypes.length)
                    .filter(x -> keyList.contains(x));
        } else {
            stream = IntStream.range(0, fieldDataTypes.length);
        }

        return stream
                .mapToObj(x -> RowData.createFieldGetter(fieldDataTypes[x].getLogicalType(), x))
                .map(e -> e.getFieldOrNull(rowData))
                .map(v -> {
                    Object o = v;
                    if (v instanceof DecimalData) {
                        o = ((DecimalData) v).toBigDecimal();
                    } else if (v instanceof StringData) {
                        o = new String(((StringData) v).toBytes());
                    }
                    return o;
                }).collect(Collectors.toList());
    }

    /**
     * 设置 PreparedStatement 参数
     *
     * @param value
     * @throws SQLException
     */
    private void setPstmtParams(RowData value, PreparedStatement pstmt, boolean KeyFilter) throws Exception {
        if (pstmt != null) {
            pstmt.clearParameters();
            List<Object> values = getValues(value, KeyFilter);
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i));
            }
        }
    }


    @Override
    public void close() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }
}
