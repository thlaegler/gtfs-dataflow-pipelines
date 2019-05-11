package com.laegler.gtfs.statik;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.laegler.gtfs.common.GtfsBigQueryTableOptions;
import com.laegler.gtfs.common.GtfsOptions;
import com.laegler.gtfs.common.GtfsUtils;
import com.laegler.gtfs.common.SubProcessConfiguration;
import com.laegler.gtfs.statik.model.Trip;

/**
 * A streaming Beam Gtfs using BigQuery output.
 *
 * <p>
 * This pipeline example reads lines of the input text file, splits each line into individual words,
 * capitalizes those words, and writes the output to a BigQuery table.
 *
 * <p>
 * The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@literal
 * --bigQueryDataset}, and {@literal --bigQueryTable} options. If the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>
 * The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C) and
 * then exits.
 */
public class ImportGtfsZip {

  /** A {@link DoFn} that tokenizes lines of text into individual words. */
  static class ExtractWords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] words = c.element().split(GtfsUtils.TOKENIZER_PATTERN, -1);

      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /** A {@link DoFn} that uppercases a word. */
  static class Uppercase extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toUpperCase());
    }
  }

  /** A {@link DoFn} that uppercases a word. */
  static class Uppercase2 extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toUpperCase());
    }
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

  /**
   * Options supported by {@link StreamingWordExtract}.
   *
   * <p>
   * Inherits standard configuration options.
   */
  public interface StreamingWordExtractOptions
      extends GtfsOptions, GtfsBigQueryTableOptions, StreamingOptions {

    @Override
    @Description("Path of the directory to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/")
    String getInput();

    @Override
    void setInput(String value);
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    StreamingWordExtractOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(StreamingWordExtractOptions.class);
    options.setStreaming(true);

    // SubProcessPipelineOptions options =
    // PipelineOptionsFactory.fromArgs(args).withValidation().as(SubProcessPipelineOptions.class);
    // SubProcessConfiguration configuration = options.getSubProcessConfiguration();

    Pipeline p = Pipeline.create(options);

    // PCollection<KV<String, Long>> filteredWords =
    // p.apply("Load GTFS zip file from storage bucket and unzip it",
    // TextIO.read().from(options.getInput()).withCompression(Compression.ZIP))
    // .apply("Echo inputs round 1", ParDo.of(new EchoInputDoFn<InputT, OutputT>(configuration,
    // "Echo")).apply(new WordCount.CountWords())
    // .apply(ParDo.of(new FilterTextFn(options.getFilterPattern())));


    String tableSpec = new StringBuilder().append(options.getProject()).append(":")
        .append(options.getBigQueryDataset()).append(".").append(options.getBigQueryTable())
        .toString();

    // PCollection<String> csvFiles = pipeline
    // .apply("Load GTFS zip file from storage bucket and unzip it",
    // TextIO.read().from(options.getInput()).withCompression(Compression.ZIP));
    // tables = csvFiles.apply("Generate SQL statements", );

    // csvFiles.apply(TextIO.).apply("", JdbcIO.<KV<Integer, String>>write()
    // .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
    // "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
    // .withUsername("username")
    // .withPassword("password"))
    // .withStatement("insert into Person values(?, ?)")
    // .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
    //
    // @Override
    // public void setParameters(KV<Integer, String> element, PreparedStatement query)
    // throws Exception {
    // query.setInt(1, kv.getKey());
    // query.setString(2, kv.getValue());
    // }
    // })
    // );
    // csvFiles.apply("Connect to Postgres Database", JdbcIO.<KV<Integer, String>>write()
    // .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
    // .create("com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
    // .withUsername("username").withPassword("password"))
    // .withStatement("INSERT INTO trips SET").withBatchSize(1000)).
    // .withStatement("INSERT INTO trips SET")
    // .apply(TextIO.write().to("gs://bucket/dump.sql").withoutSharding());

    PCollection<String> csvs =
        p.apply(TextIO.read().from(options.getInput()).withCompression(Compression.ZIP));
    // .apply(WithKeys<String,String>.of(getKey()));
    PCollection<Trip> trips = csvs.apply(ParDo.of(new DoFn<String, Trip>() {
      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(ProcessContext c) {
        String[] strArr2 = c.element().split(",");
        String header = Arrays.toString(strArr2);
        Trip trip = new Trip();
        trip.setTripId(strArr2[0].trim());
        // trip.setCatlibCode(strArr2[1].trim());
        // trip.setrNR(Double.valueOf(strArr2[2].trim().replace("", "0")));
        // trip.setrNCS(Double.valueOf(strArr2[3].trim().replace("", "0")));
        c.output(trip);
      }
    }));

    PDone data = trips.apply("Connect to Postgres Database",
        JdbcIO.<Trip>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create("com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
                .withUsername("username").withPassword("password"))
            .withStatement("INSERT INTO trips SET").withBatchSize(1000));


    // @formatter:off
    p
      .apply("ReadLines", TextIO.read().from(options.getInput()))
      .apply(ParDo.of(new ExtractWords()))
      .apply(ParDo.of(new Uppercase()))
      .apply(ParDo.of(new StringToRowConverter()))
      .apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(StringToRowConverter.getSchema()));
    // @formatter:on

    PipelineResult result = p.run();

    // GtfsUtils will try to cancel the pipeline before the program exists.
    // exampleUtils.waitToFinish(result);
  }

  /** Simple DoFn that echos the element, used as an example of running a C++ library. */
  @SuppressWarnings("serial")
  public static class EchoInputDoFn extends DoFn<KV<String, String>, KV<String, String>> {

    static final Logger LOG = LoggerFactory.getLogger(EchoInputDoFn.class);

    private SubProcessConfiguration configuration;
    private String binaryName;

    public EchoInputDoFn(SubProcessConfiguration configuration, String binary) {
      // Pass in configuration information the name of the filename of the sub-process and the level
      // of concurrency
      this.configuration = configuration;
      this.binaryName = binary;
    }

    @Setup
    public void setUp() throws Exception {
      // CallingSubProcessUtils.setUp(configuration, binaryName);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        // Our Library takes a single command in position 0 which it will echo back in the result
        // SubProcessCommandLineArgs commands = new SubProcessCommandLineArgs();
        // Command command = new Command(0, String.valueOf(c.element().getValue()));
        // commands.putCommand(command);
        //
        // // The ProcessingKernel deals with the execution of the process
        // SubProcessKernel kernel = new SubProcessKernel(configuration, binaryName);
        //
        // // Run the command and work through the results
        // List<String> results = kernel.exec(commands);
        // for (String s : results) {
        // c.output(KV.of(c.element().getKey(), s));
        // }
      } catch (Exception ex) {
        LOG.error("Error processing element ", ex);
        throw ex;
      }
    }
  }

}
