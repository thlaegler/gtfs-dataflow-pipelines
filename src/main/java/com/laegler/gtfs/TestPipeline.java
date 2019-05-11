package com.laegler.gtfs;

import static com.laegler.gtfs.common.GtfsDataStructureUtil.HEADERS;
import static com.laegler.gtfs.common.GtfsDataStructureUtil.SCHEMAS;
import static com.laegler.gtfs.common.GtfsDataStructureUtil.TRIPS;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.laegler.gtfs.common.TestOptions;

/**
 * Just to test and proof different Apache Beam features
 */
public class TestPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(TestPipeline.class);

  public static void main(String[] args) throws IOException {
    // Init
    TestOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TestOptions.class);
    options.setStreaming(true);
    Pipeline p = Pipeline.create(options);

    // Log pipeline start
    // POutput start = p.apply("Log pipeline start", new LogTransform());

    // Load GTFS static zip file
    PCollection<String> csvFiles = p.apply("Load GTFS static zip file",
        TextIO.read().from(options.getInputFile()).withCompression(Compression.ZIP));

    // For each CSV file
    PCollection<String> csvLines =
        csvFiles.apply("For each CSV file", ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String line = c.element();
            for (String word : line.split("[^a-zA-Z']+")) {
              c.output(word);
            }
          }
        }));

    // Transform CSV lines to table rows
    PCollection<Row> tableRows =
        csvLines.apply("Transform CSV lines to table rows", ParDo.of(new CsvLineToRowDoFn()))
            .setRowSchema(SCHEMAS.get(TRIPS));

    // Import table rows into database
    PDone databaseRowImport = tableRows.apply("Connect to Postgres Database", JdbcIO.<Row>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver",
            "jdbc:postgresql://google/postgres?useSSL=false&socketFactory=com.google.cloud.sql.postgres.SocketFactory&cloudSqlInstance=mobilityos-dev:australia-southeast:mobilityos-dev-postgres-gtfs&ApplicationName=dataflow")
            .withUsername("username").withPassword("password"))
        .withStatement("INSERT INTO trips SET")
        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Row>() {

          @Override
          public void setParameters(Row element, PreparedStatement query) throws Exception {
            // TODO
            // for( element.getValues()) {
            // }
            // element.getValues().stream().forEach(e -> query.setString(1, e.getString(1)));
            // query.setString(2, element.getString(idx));
          }
        }).withBatchSize(1000));


    // @formatter:off
    p
      .apply("ReadLines", TextIO.read().from(options.getInputFile()).withCompression(Compression.ZIP))
//      .apply(ParDo.of(new ExtractWords()))
//      .apply(ParDo.of(new Uppercase()))
//      .apply(ParDo.of(new StringToRowConverter()))
//      .apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(StringToRowConverter.getSchema()))
      ;
    // @formatter:on

    PipelineResult result = p.run();
    LOG.info("Result: {}", result);
    State state = result.waitUntilFinish();
    LOG.info("State: {}", state);
  }

  public static class CsvLineToRowDoFn extends DoFn<String, Row> {

    private static final long serialVersionUID = -8035567111641307749L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (!c.element().equalsIgnoreCase(HEADERS.get(TRIPS).get(0))) {
        List<String> vals = Arrays.asList(c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));
        final Row.Builder rowBuilder = Row.withSchema(SCHEMAS.get(TRIPS));
        vals.forEach(v -> rowBuilder.addValue(v));
        Row row = rowBuilder.build();
        c.output(row);
      }
    }
  }

  public static class RowToCsvLineDoFn extends DoFn<Row, String> {

    private static final long serialVersionUID = 2712756079230134027L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String line =
          c.element().getValues().stream().map(Object::toString).collect(Collectors.joining(","));
      c.output(line);
    }
  }

  public static class LogEachDoFn extends DoFn<String, String> {

    // static final Logger LOG = LoggerFactory.getLogger(LogEachDoFn.class);

    /**
     * 
     */
    private static final long serialVersionUID = 8638140305376717061L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String element = c.element();
      c.output(element);
      LOG.info("Elements: {}", element);
    }
  }

  public static class LogTransform extends PTransform<PInput, POutput> {

    // static final Logger LOG = LoggerFactory.getLogger(LogEachDoFn.class);

    /**
     * 
     */
    private static final long serialVersionUID = -6234205534828681283L;

    @Override
    public POutput expand(PInput input) {
      LOG.info("Input: {}", input);
      return (POutput) input;
    }
  }

}
