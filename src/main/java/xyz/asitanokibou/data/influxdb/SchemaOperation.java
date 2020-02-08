package xyz.asitanokibou.data.influxdb;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.annotation.Nonnull;
import java.util.*;
/**
 * @author aimysaber@gmail.com
 */
@Slf4j
public class SchemaOperation {
    //https://docs.influxdata.com/influxdb/v1.7/query_language/database_management/

    private static final String SHOW_TAG_KEY = "SHOW TAG KEYS";
    private static final String SHOW_TAG_VALUES = "SHOW TAG VALUES";
    private static final String SHOW_MEASUREMENTS = "SHOW MEASUREMENTS";

    private final InfluxDBTemplate influxDBTemplate;

    public SchemaOperation(InfluxDBTemplate influxDBTemplate) {
        this.influxDBTemplate = influxDBTemplate;
    }

    public Map<String, List<String>> getTagKeys(String database, String measurement) {

        Objects.requireNonNull(database, "database must be specified");

        return influxDBTemplate.queryForObject(() -> {
            StringBuilder sb = new StringBuilder(SHOW_TAG_KEY);
            sb.append(" ON ").append(database);

            if (Utils.isNotEmpty(measurement)) {
                sb.append(" FROM ").append(measurement);
            }
            return new Query(sb.toString(), database);

        }, (QueryResult queryResult) -> {
            Map<String, List<String>> resultMap = new HashMap<>(queryResult.getResults().size(), 1);

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {
                String name = series.getName();//measurement name
                List<String> tagKeyList = resultMap.computeIfAbsent(name, k -> new ArrayList<>());
                series.getValues().forEach(ser -> {
                    ser.forEach(s -> tagKeyList.add(String.valueOf(s)));
                    //tagKeyList.addAll((List)ser); <-- 这句会导致下面的.orElse(Collections.emptyMap());报错！
                });
            });

            return resultMap;

        }).orElse(Collections.emptyMap());
    }

   public Map<String, List<String>> getTagValues(String database, String measurement, String tagKey) {
        Objects.requireNonNull(database, "database must be specified");

        return influxDBTemplate.queryForObject(() -> {
            StringBuilder sb = new StringBuilder(SHOW_TAG_VALUES);
            sb.append(" ON ").append(database);
            if (Utils.isNotEmpty(measurement)) {
                sb.append(" FROM ").append(measurement);
            }
            sb.append(" WITH KEY = ").append(Utils.quote(tagKey));
            return new Query(sb.toString(), database);

        }, (QueryResult queryResult) -> {

            Map<String, List<String>> resultMap = new HashMap<>(queryResult.getResults().size(), 1);

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {
                String name = series.getName();//measurement name , columns: [key,value]
                List<String> tagKeyList = resultMap.computeIfAbsent(name, k -> new ArrayList<>());

                for (List<Object> ser : series.getValues()) { //List<Object> ser <= 0-key(tagKey) , 1-value
                    tagKeyList.add(String.valueOf(ser.get(1)));
                }
            });
            return resultMap;

        }).orElse(Collections.emptyMap());

    }


    public void dropMeasurement(@Nonnull String databse, String measurement){
        //为了谨慎起见 必须给定database 防止删除了不同数据库(database)的measurement
        Objects.requireNonNull(databse, "database must be specified");
        influxDBTemplate.execute(client -> client.query(new Query("DROP MEASUREMENT " + measurement, databse)));
    }

    public List<String> getMeasurements() {
        return getMeasurements(null);
    }

    @SuppressWarnings("unchecked")
    //暂不支持where
    public List<String> getMeasurements(String database) {
        List<List<String>> resultList =
                influxDBTemplate.queryForList(() -> new Query(SHOW_MEASUREMENTS, database), series -> Utils.collcect((List) series.getValues()));

        return Utils.first(resultList).orElse(Collections.emptyList());
    }
}
