package com.laegler.gtfs.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options that can be used to configure the Beam examples. */
public interface GtfsOptions extends PipelineOptions {

  @Description("Whether to keep jobs running after local process exit")
  @Default.Boolean(false)
  boolean getKeepJobsRunning();

  void setKeepJobsRunning(boolean keepJobsRunning);

  @Description("Number of workers to use when executing the injector pipeline")
  @Default.Integer(1)
  int getInjectorNumWorkers();

  void setInjectorNumWorkers(int numWorkers);

  @Description("Path of the directory to read from")
  @Default.String("gs://apache-beam-samples/shakespeare/")
  String getInput();

  void setInput(String value);

  @Description("Path of the directory to write to")
  @Default.String("gs://apache-beam-samples/shakespeare/")
  String getOutput();

  void setOutput(String value);

  // @Description("Path of the directory to read from")
  // @Default.String("gs://apache-beam-samples/shakespeare/")
  // String getProject();
  //
  // void setProject(String value);

}
