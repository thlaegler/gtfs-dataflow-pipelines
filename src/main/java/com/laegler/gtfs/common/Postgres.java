package com.laegler.gtfs.common;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class Postgres {
  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    StreamingGtfsRealtimePostgresOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(StreamingGtfsRealtimePostgresOptions.class);
    options.setStreaming(true);

    options.setBigQuerySchema(StringToRowConverter.getSchema());
    GtfsUtils exampleUtils = new GtfsUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(JdbcIO.<KV<Integer, String>>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
            .create("com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
            .withUsername("username").withPassword("password"))
        .withQuery("select id,name from Person where name = ?")
        // .withQuery("INSERT INTO trips SET")
        .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
        .withStatementPreparator(new JdbcIO.StatementPreparator() {
          @Override
          public void setParameters(PreparedStatement preparedStatement) throws Exception {
            preparedStatement.setString(1, "Darwin");
          }
        }).withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
          @Override
          public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
            return KV.of(resultSet.getInt(1), resultSet.getString(2));
          }
        }));
  }

  /**
   * Options supported by {@link StreamingWordExtract}.
   *
   * <p>
   * Inherits standard configuration options.
   */
  public interface StreamingGtfsRealtimePostgresOptions
      extends GtfsOptions, GtfsBigQueryTableOptions, StreamingOptions {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);
  }

  /** Converts strings into BigQuery rows. */
  static class StringToRowConverter extends DoFn<String, TableRow> {
    /** In this example, put the whole string into single BigQuery field. */
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new TableRow().set("string_field", c.element()));
    }

    static TableSchema getSchema() {
      return new TableSchema().setFields(Collections
          .singletonList(new TableFieldSchema().setName("string_field").setType("STRING")));
    }
  }

  public static void fixWrongColumnNames() {

  }
}
