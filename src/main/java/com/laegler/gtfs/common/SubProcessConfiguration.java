package com.laegler.gtfs.common;

import java.io.Serializable;

/**
 * Configuration file used to setup the Process kernel for execution of the external library Values
 * are copied from the Options to all them to be Serializable.
 */
@SuppressWarnings("serial")
public class SubProcessConfiguration implements Serializable {

  // Source GCS directory where the C++ library is located gs://bucket/tests
  public String sourcePath;

  // Working directory for the process I/O
  public String workerPath;

  // The maximum time to wait for the sub-process to complete
  public Integer waitTime;

  // "As sub-processes can be heavy weight match the concurrency level to num cores on the machines"
  public Integer concurrency;

  // Should log files only be uploaded if error
  public Boolean onlyUpLoadLogsOnError;

  public Boolean getOnlyUpLoadLogsOnError() {
    return onlyUpLoadLogsOnError;
  }

  public void setOnlyUpLoadLogsOnError(Boolean onlyUpLoadLogsOnError) {
    this.onlyUpLoadLogsOnError = onlyUpLoadLogsOnError;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }

  public String getWorkerPath() {
    return workerPath;
  }

  public void setWorkerPath(String workerPath) {
    this.workerPath = workerPath;
  }

  public Integer getWaitTime() {
    return waitTime;
  }

  public void setWaitTime(Integer waitTime) {
    this.waitTime = waitTime;
  }

  public Integer getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(Integer concurrency) {
    this.concurrency = concurrency;
  }
}