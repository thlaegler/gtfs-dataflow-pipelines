package com.laegler.gtfs.statik.model;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;
import java.io.Serializable;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonInclude(ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "tripId")
public class Trip implements Serializable {

  private static final long serialVersionUID = 5145447946859483604L;

  private String tripId;

  private String secondaryTripId;

  private Set<String> stopTimes;

  private String shapeId;

  private String routeId;

  private String secondaryRouteId;

  private String serviceId;

  // private LineString polyline;

  private String headsign;

  private String shortName;

  private String blockId;

  public String getTripId() {
    return tripId;
  }

  public void setTripId(String tripId) {
    this.tripId = tripId;
  }

  public String getSecondaryTripId() {
    return secondaryTripId;
  }

  public void setSecondaryTripId(String secondaryTripId) {
    this.secondaryTripId = secondaryTripId;
  }

  public Set<String> getStopTimes() {
    return stopTimes;
  }

  public void setStopTimes(Set<String> stopTimes) {
    this.stopTimes = stopTimes;
  }

  public String getShapeId() {
    return shapeId;
  }

  public void setShapeId(String shapeId) {
    this.shapeId = shapeId;
  }

  public String getRouteId() {
    return routeId;
  }

  public void setRouteId(String routeId) {
    this.routeId = routeId;
  }

  public String getSecondaryRouteId() {
    return secondaryRouteId;
  }

  public void setSecondaryRouteId(String secondaryRouteId) {
    this.secondaryRouteId = secondaryRouteId;
  }

  public String getServiceId() {
    return serviceId;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public String getHeadsign() {
    return headsign;
  }

  public void setHeadsign(String headsign) {
    this.headsign = headsign;
  }

  public String getShortName() {
    return shortName;
  }

  public void setShortName(String shortName) {
    this.shortName = shortName;
  }

  public String getBlockId() {
    return blockId;
  }

  public void setBlockId(String blockId) {
    this.blockId = blockId;
  }

  // private DirectionType directionType;
  //
  // private WheelchairAccessability wheelchairAccessible;
  //
  // private BicycleAllowance bikesAllowed;


}

