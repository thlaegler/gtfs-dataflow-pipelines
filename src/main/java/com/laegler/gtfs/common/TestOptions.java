package com.laegler.gtfs.common;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;

public interface TestOptions extends GcpOptions, StreamingOptions {

  @Description("Storage bucket path to the GTFS static zip-file to be imported.")
  @Default.String("gs://mobilityos-dev-gtfs/at/20190429120000/at.zip")
  String getInputFile();

  void setInputFile(String value);

  @Description("Storage bucket path to the GTFS static zip-file that'll get exported.")
  @Default.String("gs://mobilityos-dev-opentripplanner/output/")
  String getOutputFile();

  void setOutputFile(String value);

  @Description("Whether to export a true schedule based on history or to export static stop times")
  @Default.Boolean(false)
  boolean getExportTrueSchedule();

  void setExportTrueSchedule(boolean exportTrueSchedule);

}
