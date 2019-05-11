package com.laegler.gtfs.common;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableList;

/**
 * Because many GTFS Provider don't stick exactly to the GTFS Specification but rather use custom
 * tables and column names we need to map the table and column names to proper GTFS standard.
 */
public class GtfsDataStructureUtil {

  private static final Logger LOG = LoggerFactory.getLogger(GtfsDataStructureUtil.class);

  // Table names
  public static final String AGENCIES = "agency";
  public static final String CALENDARS = "calendar";
  public static final String CALENDAR_DATES = "calendar_dates";
  public static final String FARE_ATTRIBUTES = "fare_attributes";
  public static final String FARE_RULES = "fare_rules";
  public static final String FEED_INFOS = "feed_info";
  public static final String FREQUENCIES = "frequencies";
  public static final String ROUTES = "routes";
  public static final String SHAPES = "shapes";
  public static final String STOP_TIMES = "stop_times";
  public static final String STOPS = "stops";
  public static final String TRANSFERS = "transfers";
  public static final String TRIPS = "trips";

  // Table names of Extensions or GTFS-Spec-Updates
  public static final String TRANSLATIONS = "translations";
  public static final String PATHWAYS = "pathways";
  public static final String LEVELS = "levels";
  public static final String CALENDAR_ATTRIBUTES = "calendar_attributes";
  public static final String DIRECTIONS = "directions";

  // this defines the order
  public static final List<String> GTFS_TABLES = asList(AGENCIES, ROUTES, FARE_ATTRIBUTES,
      FARE_RULES, STOPS, SHAPES, FEED_INFOS, CALENDARS, CALENDAR_DATES, TRIPS, FREQUENCIES,
      STOP_TIMES, TRANSFERS, TRANSLATIONS, PATHWAYS, LEVELS, CALENDAR_ATTRIBUTES, DIRECTIONS);

  public static final String AGENCY_ID = "agency_id";
  public static final String SERVICE_ID = "service_id";
  public static final String SECONDARY_SERVICE_ID = "secondary_service_id";
  public static final String FARE_ID = "fare_id";
  public static final String SECONDARY_FARE_ID = "secondary_fare_id";
  public static final String ROUTE_ID = "route_id";
  public static final String SECONDARY_ROUTE_ID = "secondary_route_id";
  public static final String TRIP_ID = "trip_id";
  public static final String SECONDARY_TRIP_ID = "secondary_trip_id";
  public static final String STOP_ID = "stop_id";
  public static final String SECONDARY_STOP_ID = "secondary_stop_id";
  public static final String SHAPE_ID = "shape_id";
  public static final String SECONDARY_SHAPE_ID = "secondary_shape_id";
  public static final String FREQUENCY_ID = "frequency_id";
  public static final String SECONDARY_FREQUENCY_ID = "secondary_frequency_id";
  public static final String TRANSFER_ID = "transfer_id";
  public static final String TRANS_ID = "trans_id";
  public static final String PATHWAY_ID = "pathway_id";
  public static final String LEVEL_ID = "level_id";
  public static final String DIRECTION_ID = "direction_id";

  public static final List<List<String>> TABLE_ALIASES = new LinkedList<>();

  // The first column name is the GTFS default name, all following are aliases that are used by some
  // providers.
  public static final Map<String, List<List<String>>> COLUMN_ALIASES = new HashMap<>();

  public static final Map<String, String> UNIQUE_CONSTRAINTS = new HashMap<>();

  public static final Map<String, ImmutablePair<String, String>> SECONDARY_ID_COLUMNS =
      new HashMap<>();

  public static final Map<String, List<ImmutablePair<String, String>>> TIME_TRANSFORMATION =
      new HashMap<>();


  // Apache Beam specific
  public static final Map<String, Schema> SCHEMAS = new HashMap<>();

  public static final Map<String, String> UNIQUE_HEADER = new HashMap<>(); // to detect table name
  // by header
  public static final Map<String, List<String>> HEADERS = new HashMap<>();

  // Static initialization
  static {
    TABLE_ALIASES.add(ImmutableList.of(AGENCIES, "agencies"));
    TABLE_ALIASES.add(ImmutableList.of(CALENDARS, "calendars"));
    TABLE_ALIASES.add(ImmutableList.of(CALENDAR_DATES));
    TABLE_ALIASES.add(ImmutableList.of(FARE_ATTRIBUTES));
    TABLE_ALIASES.add(ImmutableList.of(FARE_RULES));
    TABLE_ALIASES.add(ImmutableList.of(FEED_INFOS));
    TABLE_ALIASES.add(ImmutableList.of(FREQUENCIES));
    TABLE_ALIASES.add(ImmutableList.of(ROUTES));
    TABLE_ALIASES.add(ImmutableList.of(SHAPES));
    TABLE_ALIASES.add(ImmutableList.of(STOP_TIMES));
    TABLE_ALIASES.add(ImmutableList.of(STOPS));
    TABLE_ALIASES.add(ImmutableList.of(TRANSFERS, "transfer"));
    TABLE_ALIASES.add(ImmutableList.of(TRIPS));
    TABLE_ALIASES.add(ImmutableList.of(TRANSLATIONS));
    TABLE_ALIASES.add(ImmutableList.of(PATHWAYS));
    TABLE_ALIASES.add(ImmutableList.of(LEVELS));
    TABLE_ALIASES.add(ImmutableList.of(CALENDAR_ATTRIBUTES));
    TABLE_ALIASES.add(ImmutableList.of(DIRECTIONS, "direction"));

    UNIQUE_CONSTRAINTS.put(AGENCIES, "agency_pkey");
    UNIQUE_CONSTRAINTS.put(CALENDARS, "calendar_pkey");
    UNIQUE_CONSTRAINTS.put(CALENDAR_DATES, "calendar_dates_pkey");
    UNIQUE_CONSTRAINTS.put(FARE_ATTRIBUTES, "fare_attributes_pkey");
    UNIQUE_CONSTRAINTS.put(FARE_RULES, "fare_rules_pkey"); // idx_fare_rules_composite_unique
    UNIQUE_CONSTRAINTS.put(FEED_INFOS, "feed_info_pkey");
    UNIQUE_CONSTRAINTS.put(FREQUENCIES, "frequencies_pkey");
    UNIQUE_CONSTRAINTS.put(ROUTES, "routes_pkey");
    UNIQUE_CONSTRAINTS.put(SHAPES, "shapes_pkey"); // idx_shapes_shape_id_sequence_unique
    UNIQUE_CONSTRAINTS.put(STOP_TIMES, "idx_stop_times_unique"); // idx_stop_times_unique
    UNIQUE_CONSTRAINTS.put(STOPS, "stops_pkey");
    UNIQUE_CONSTRAINTS.put(TRANSFERS, "transfers_pkey");
    UNIQUE_CONSTRAINTS.put(TRIPS, "trips_pkey");
    UNIQUE_CONSTRAINTS.put(TRANSLATIONS, "translations_pkey");
    UNIQUE_CONSTRAINTS.put(PATHWAYS, "pathways_pkey");
    UNIQUE_CONSTRAINTS.put(LEVELS, "levels_pkey");
    UNIQUE_CONSTRAINTS.put(CALENDAR_ATTRIBUTES, "calendar_attributes_pkey");
    UNIQUE_CONSTRAINTS.put(DIRECTIONS, "directions_pkey");

    COLUMN_ALIASES.put(AGENCIES,
        asList(asList(AGENCY_ID), asList("agency_lang"), asList("agency_name"),
            asList("agency_phone"), asList("agency_timezone"), asList("agency_email"),
            asList("agency_url"), asList("agency_fare_url")));

    COLUMN_ALIASES.put(CALENDARS,
        asList(asList(SERVICE_ID), asList(SECONDARY_SERVICE_ID), asList("service_name"),
            asList("start_date"), asList("end_date"), asList("monday"), asList("tuesday"),
            asList("wednesday"), asList("thursday"), asList("friday"), asList("saturday"),
            asList("sunday")));

    COLUMN_ALIASES.put(CALENDAR_DATES,
        asList(asList("calendar_date_id"), asList(SERVICE_ID), asList(SECONDARY_SERVICE_ID),
            asList("date", "calendar_date"), asList("exception_type"), asList("holiday_name")));

    COLUMN_ALIASES.put(FARE_ATTRIBUTES,
        asList(asList(FARE_ID), asList(SECONDARY_FARE_ID), asList("currency_type"),
            asList("payment_method"), asList(AGENCY_ID), asList("price"),
            asList("transfer_duration"), asList(TRANSFERS), asList("transfer_type"),
            asList("extra_fare_info"), asList("value_options"), asList("transfers")));

    COLUMN_ALIASES.put(FARE_RULES, asList(asList(FARE_ID), asList(SECONDARY_FARE_ID),
        asList(ROUTE_ID), asList("origin_id"), asList("destination_id"), asList("contains_id")));

    COLUMN_ALIASES.put(FEED_INFOS,
        asList(asList("feed_id"), asList("feed_publisher_name"), asList("feed_start_date"),
            asList("feed_end_date"), asList("feed_lang"), asList("feed_version"),
            asList("feed_publisher_url")));

    COLUMN_ALIASES.put(FREQUENCIES,
        asList(asList(FREQUENCY_ID), asList(SECONDARY_FREQUENCY_ID), asList(TRIP_ID),
            asList("start_time"), asList("end_time"), asList("exact_times", "exact_time"),
            asList("headway_secs", "headway_sec")));

    COLUMN_ALIASES.put(ROUTES,
        asList(asList(ROUTE_ID), asList(SECONDARY_ROUTE_ID), asList(AGENCY_ID),
            asList("route_short_name"), asList("route_long_name"), asList("route_desc"),
            asList("route_type"), asList("route_url"), asList("route_color"),
            asList("route_text_color"), asList("route_sort_order", "sort_order"),
            asList("min_headway_minutes"), asList("eligibility_restricted")));

    COLUMN_ALIASES.put(SHAPES,
        asList(asList(SHAPE_ID), asList(SECONDARY_SHAPE_ID), asList("shape_pt_sequence"),
            asList("shape_pt_lat"), asList("shape_pt_lon"), asList("shape_dist_traveled")));

    COLUMN_ALIASES.put(STOP_TIMES,
        asList(asList("stop_time_id"), asList(STOP_ID), asList(TRIP_ID), asList("stop_sequence"),
            asList("arrival_time"), asList("departure_time"), asList("stop_headsign"),
            asList("pickup_type"), asList("drop_off_type"), asList("shape_dist_traveled"),
            asList("timepoint"), asList("start_service_area_id"), asList("end_service_area_id"),
            asList("start_service_area_radius"), asList("end_service_area_radius"),
            asList("continuous_pickup"), asList("continuous_drop_off"), asList("pickup_area_id"),
            asList("drop_off_area_id"), asList("pickup_service_area_radius"),
            asList("drop_off_service_area_radius"), asList("major_stop")));

    COLUMN_ALIASES.put(STOPS,
        asList(asList(STOP_ID), asList(SECONDARY_STOP_ID), asList("stop_code"), asList("stop_name"),
            asList("stop_desc"), asList("stop_lat"), asList("stop_lon"), asList("zone_id"),
            asList("stop_url"), asList("location_type"), asList("parent_station"),
            asList("stop_timezone"), asList("wheelchair_boarding", "wheelchair_type"),
            asList("platform_code"), asList("vehicle_type")));

    COLUMN_ALIASES.put(TRANSFERS,
        asList(asList(TRANSFER_ID), asList("from_stop_id"), asList("to_stop_id"),
            asList("transfer_type"), asList("min_transfer_time", "min_transfer"),
            asList("from_trip_id"), asList("to_trip_id")));

    COLUMN_ALIASES.put(TRIPS,
        asList(asList(TRIP_ID), asList(SECONDARY_TRIP_ID), asList(ROUTE_ID), asList(SHAPE_ID),
            asList(SERVICE_ID), asList("direction_id"), asList("block_id"),
            asList("wheelchair_accessible", "trip_type"), asList("bikes_allowed"),
            asList("trip_short_name"), asList("trip_headsign"), asList("drt_max_travel_time"),
            asList("drt_avg_travel_time"), asList("drt_advance_book_min"),
            asList("drt_pickup_message"), asList("drt_drop_off_message"),
            asList("continuous_pickup_message"), asList("continuous_drop_off_message")));

    COLUMN_ALIASES.put(TRANSLATIONS,
        asList(asList(TRANS_ID), asList("lang"), asList("translation")));

    COLUMN_ALIASES.put(PATHWAYS,
        asList(asList("pathway_id"), asList("from_stop_id"), asList("to_stop_id"),
            asList("pathway_mode"), asList("is_bidirectional"), asList("length"),
            asList("traversal_time"), asList("stair_count"), asList("max_slope"),
            asList("min_width"), asList("signposted_as"), asList("reversed_signposted_as")));

    COLUMN_ALIASES.put(LEVELS,
        asList(asList("level_id"), asList("level_index"), asList("level_name")));

    COLUMN_ALIASES.put(CALENDAR_ATTRIBUTES,
        asList(asList(SERVICE_ID), asList(SECONDARY_SERVICE_ID), asList("service_description")));

    COLUMN_ALIASES.put(DIRECTIONS,
        asList(asList(ROUTE_ID), asList(DIRECTION_ID), asList("direction")));

    SECONDARY_ID_COLUMNS.put(CALENDARS, ImmutablePair.of(SERVICE_ID, SECONDARY_SERVICE_ID));
    SECONDARY_ID_COLUMNS.put(CALENDAR_DATES, ImmutablePair.of(SERVICE_ID, SECONDARY_SERVICE_ID));
    SECONDARY_ID_COLUMNS.put(FARE_ATTRIBUTES, ImmutablePair.of(FARE_ID, SECONDARY_FARE_ID));
    SECONDARY_ID_COLUMNS.put(FARE_RULES, ImmutablePair.of(FARE_ID, SECONDARY_FARE_ID));
    SECONDARY_ID_COLUMNS.put(ROUTES, ImmutablePair.of(ROUTE_ID, SECONDARY_ROUTE_ID));
    SECONDARY_ID_COLUMNS.put(SHAPES, ImmutablePair.of(SHAPE_ID, SECONDARY_SHAPE_ID));
    SECONDARY_ID_COLUMNS.put(STOPS, ImmutablePair.of(STOP_ID, SECONDARY_STOP_ID));
    SECONDARY_ID_COLUMNS.put(TRIPS, ImmutablePair.of(TRIP_ID, SECONDARY_TRIP_ID));
    SECONDARY_ID_COLUMNS.put(STOP_TIMES, ImmutablePair.of(TRIP_ID, SECONDARY_TRIP_ID));
    SECONDARY_ID_COLUMNS.put(FREQUENCIES, ImmutablePair.of(FREQUENCY_ID, SECONDARY_FREQUENCY_ID));
    SECONDARY_ID_COLUMNS.put(CALENDAR_ATTRIBUTES,
        ImmutablePair.of(SERVICE_ID, SECONDARY_SERVICE_ID));

    TIME_TRANSFORMATION.put(STOP_TIMES,
        asList(ImmutablePair.of("arrival_time", "arrival_time_time"),
            ImmutablePair.of("departure_time", "departure_time_time")));

    TIME_TRANSFORMATION.put(FREQUENCIES, asList(ImmutablePair.of("start_time", "start_time_time"),
        ImmutablePair.of("end_time", "end_time_time")));

    // Apache Beam specific

    SCHEMAS.put(AGENCIES,
        Schema.builder().addStringField("route_id").addStringField("agency_id").build());
    final Schema.Builder schemaBuilder = Schema.builder();

    // @formatter:off
    COLUMN_ALIASES.entrySet().forEach(e -> 
      SCHEMAS.put(e.getKey(), 
        new Schema(
          e.getValue().stream().map(col -> 
            Schema.Field.of(
              col.get(0),
              FieldType.STRING
            )
          ).collect(toList())
        )
      )
    );
    // @formatter:on

    SCHEMAS.put(CALENDARS,
        Schema.builder().addStringField("route_id").addStringField("agency_id").build());

    // HEADERS = ImmutableList.of(AGENCIES,
    // Schema.builder().addStringField("route_id").addStringField("agency_id").build()));
    // SCHEMAS.add(ImmutableList.of(CALENDARS, "calendars"));
    // SCHEMAS.add(ImmutableList.of(CALENDAR_DATES));
    // SCHEMAS.add(ImmutableList.of(FARE_ATTRIBUTES));
    // SCHEMAS.add(ImmutableList.of(FARE_RULES));
    // SCHEMAS.add(ImmutableList.of(FEED_INFOS));
    // SCHEMAS.add(ImmutableList.of(FREQUENCIES));
    // SCHEMAS.add(ImmutableList.of(ROUTES));
    // SCHEMAS.add(ImmutableList.of(SHAPES));
    // SCHEMAS.add(ImmutableList.of(STOP_TIMES));
    // SCHEMAS.add(ImmutableList.of(STOPS));
    // SCHEMAS.add(ImmutableList.of(TRANSFERS, "transfer"));
    // SCHEMAS.add(ImmutableList.of(TRIPS));
    // SCHEMAS.add(ImmutableList.of(TRANSLATIONS));
    // SCHEMAS.add(ImmutableList.of(PATHWAYS));
    // SCHEMAS.add(ImmutableList.of(LEVELS));
    // SCHEMAS.add(ImmutableList.of(CALENDAR_ATTRIBUTES));
    // SCHEMAS.add(ImmutableList.of(DIRECTIONS, "direction"));

    // TABLE_ALIASES.forEach(t -> UNIQUE_HEADER.put(t.get(0), "");
  }

  private static final String SHOULDBENULL = "SHOULDBENULL";

  private GtfsDataStructureUtil() {
    throw new IllegalStateException("Cannot instantiate this static class");
  }

  public static String csvTableNameToDatabaseTableName(final String csvTable) {
    List<String> col =
        TABLE_ALIASES.stream().filter(a1 -> a1.stream().anyMatch(m -> m.equalsIgnoreCase(csvTable)))
            .findFirst().orElse(null);

    if (col != null && col.get(0) != null) {
      return col.get(0);
    }
    return csvTable;
  }

  public static Optional<String> getTableNameFromHeader(String headerLine) {
    return UNIQUE_HEADER.entrySet().stream().filter(e -> headerLine.contains(e.getValue()))
        .map(e -> e.getKey()).findFirst();
  }

  public static List<String> csvColumnNamesToDatabaseColumnNames(String tableName,
      List<String> csvColumns) {
    final List<String> orderedColumns = new LinkedList<>();
    List<List<String>> columnAliases = COLUMN_ALIASES.get(tableName);
    if (columnAliases != null && !columnAliases.isEmpty()) {
      csvColumns.stream().forEach(csv -> {
        List<String> col =
            columnAliases.stream().filter(a1 -> a1.stream().anyMatch(m -> m.equalsIgnoreCase(csv)))
                .findFirst().orElse(null);
        if (col != null && !col.isEmpty()) {
          orderedColumns.add(col.get(0));
        }
      });
    }
    addCustomColumns(orderedColumns, tableName);
    return orderedColumns;
  }

  /**
   * GTFS allows times like 25:30:00 but Postgres data type 'time' does not allow such values
   * (equals or greater 24). This method creates a valid date. The initially invalid time stays
   * untouched in another column.
   * 
   * @param potentiallyInvalidTime a potentially invalid time as String
   * @return a valid time as String
   */
  public static String getValidTime(String potentiallyInvalidTime) {
    if (potentiallyInvalidTime != null) {
      if (potentiallyInvalidTime.equalsIgnoreCase(SHOULDBENULL)) {
        return null;
      }
      List<String> timeSegments = Arrays.asList(potentiallyInvalidTime.replace("'", "").split(":"));
      if (timeSegments != null && !timeSegments.isEmpty()) {
        try {
          int gtfsHourOfDay = Integer.parseInt(timeSegments.get(0));
          if (gtfsHourOfDay >= 24) {
            timeSegments.set(0, String.valueOf(gtfsHourOfDay - 24));
            return timeSegments.stream().collect(joining(":"));
          }
        } catch (NumberFormatException ex) {
          LOG.warn("Cannot parse to Integer {} (hour of day)", timeSegments.get(0), ex);
        }
      }
    }
    return potentiallyInvalidTime;
  }

  public static List<String> getLineValues(String line) {
    String newLine = line.replace("'", "''");
    newLine = newLine.replace("\"", "'");
    List<String> parts = asList(newLine.split(",(?=(?:[^']*'[^']*')*[^']*$)", -1));
    return parts.stream().map(p -> (p == null || p.isEmpty()) ? SHOULDBENULL : p).collect(toList());
  }

  public static String csvLineToSqlLine(List<String> lineList) {
    String line = lineList.stream().collect(joining("','", "('", "')"));
    line = line.replace("(''", "('");
    line = line.replace(",''", ",'");
    line = line.replace("'')", "')");
    line = line.replace("'',", "',");
    line = line.replace("'" + SHOULDBENULL + "'", "null");
    line = line.replace("'null'", "null");
    return line;
  }

  private static void addCustomColumns(final List<String> defaultColumns, String tableName) {
    ImmutablePair<String, String> secondaryId =
        SECONDARY_ID_COLUMNS.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(tableName))
            .map(Entry::getValue).findFirst().orElse(null);
    if (secondaryId != null) {
      defaultColumns.add(secondaryId.getRight());
    }
    if (TIME_TRANSFORMATION.containsKey(tableName)) {
      TIME_TRANSFORMATION.get(tableName).stream().forEach(t -> defaultColumns.add(t.getRight()));
    }
  }

}
