package xyz.asitanokibou.data.influxdb;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BoundParameterQuery;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import xyz.asitanokibou.data.influxdb.ex.InfluxDBXConnectionException;
import xyz.asitanokibou.data.influxdb.ex.InfluxDBXException;
import xyz.asitanokibou.data.influxdb.pojo.TimedResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author aimysaber@gmail.com
 */
@Slf4j
public class InfluxDBTemplate {

    private static final int FRACTION_MIN_WIDTH = 0;
    private static final int FRACTION_MAX_WIDTH = 9;
    private static final boolean ADD_DECIMAL_POINT = true;
    private static final DateTimeFormatter RFC3339_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, FRACTION_MIN_WIDTH, FRACTION_MAX_WIDTH, ADD_DECIMAL_POINT)
            .appendZoneOrOffsetId()
            .toFormatter();
    private InfluxDBClientPool dataSource;

    /*
        String name = series.getName();//measurement name
        List<String> columns = series.getColumns();
        Map<String, String> tags = series.getTags();
        List<List<Object> seriesValues = series.getValues();
    */
    private InfluxDBResultMapper resultMapper; //thread-safe
    private SchemaOperation schemaOperation;
    private ZoneId zoneId = ZoneId.of("Asia/Shanghai");     //ZoneId.systemDefault();

    public InfluxDBTemplate(InfluxDBClientPool influxDBClientPool) {
        this.dataSource = influxDBClientPool;
        this.resultMapper = new InfluxDBResultMapper();

        this.schemaOperation = new SchemaOperation(this);
    }

    public SchemaOperation opsForSchema() {
        return schemaOperation;
    }

    public InfluxDBClientPool getDataSource() {
        return dataSource;
    }

    public void setDataSource(InfluxDBClientPool dataSource) {
        this.dataSource = dataSource;
    }

    public <T> T execute(InfluxDBClientCallback<T> callback) {
        try (InfluxDB client = getResource()) {
            return callback.execute(client);
        }
    }

    public <T> List<T> queryForList(QueryCreator queryCreator, String measurement, Class<T> clazz) {

        return execute(client -> {
            Query query = queryCreator.createQuery();
            QueryResult queryResult = client.query(query, TimeUnit.MILLISECONDS);
            return resultMapper.toPOJO(queryResult, clazz, measurement);
        });
    }

    public <T> Optional<T> queryForObject(QueryCreator queryCreator, String measurement, Class<T> clazz) {
        List<T> resultList = queryForList(queryCreator, measurement, clazz);
        return Utils.first(resultList);
    }

    public <T> List<T> queryForListByQuery(String queryString, Class<T> clazz, Map<String, Object> argsMap) {
        return queryForListByQuery(null, null, queryString, clazz, argsMap);
    }

    public <T> List<T> queryForListByQuery(String measurement, String queryString, Class<T> clazz, Map<String, Object> argsMap) {
        return queryForListByQuery(null, measurement, queryString, clazz, argsMap);
    }

    public <T> List<T> queryForListByQuery(String database, String measurement, String queryString, Class<T> clazz, Map<String, Object> argsMap) {

        return queryForListByQuery(database, measurement, queryString, argsMap, queryResult -> {
            if (Utils.isNotEmpty(measurement)) {
                return resultMapper.toPOJO(queryResult, clazz, measurement);
            } else {
                return resultMapper.toPOJO(queryResult, clazz);
            }
        });
    }

    public <T> Optional<T> queryForObjectByQuery(String measurement, String queryString, @Nonnull Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        return queryForObjectByQuery(null, measurement, queryString, clazz, argsMap);
    }

    public <T> Optional<T> queryForObjectByQuery(@Nullable String database, @Nullable String measurement, @Nonnull String queryString, @Nonnull Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        List<T> resultList = queryForListByQuery(database, measurement, queryString, clazz, argsMap);
        return Utils.first(resultList);
    }

    /*public <T> T queryForScalar(QueryCreator queryCreator, String measurement, Class<T> clazz) {
        throw new UnsupportedOperationException("queryForTimedScalar operation is not supported now ");
    }*/

    public <T> Optional<TimedResult<T>> queryForTimedScalar(@Nullable String measurement, @Nonnull String queryString, Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        return Utils.first(queryForTimedScalarList(null, measurement, queryString, clazz, argsMap));
    }

    public <T> Optional<TimedResult<T>> queryForTimedScalar(@Nullable String database, @Nullable String measurement, @Nonnull String queryString, Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        return Utils.first(queryForTimedScalarList(database, measurement, queryString, clazz, argsMap));
    }

    @SuppressWarnings("unchecked")
    public <T> List<TimedResult<T>> queryForTimedScalarList(@Nullable String database, @Nullable String measurement, @Nonnull String queryString, Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        return queryForListByQuery(database, measurement, queryString, argsMap, queryResult -> {

            List<TimedResult<T>> results = new ArrayList<>();

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {

                //String name = series.getName();
                List<String> columns = series.getColumns();
                String columnName = columns.get(0);

                List<List<Object>> values = series.getValues();
                values.forEach(valuePairs -> {
                    Instant time = convertTimeValue(valuePairs.get(0), TimeUnit.MILLISECONDS);

                    Object rawValue = valuePairs.get(1);

                    T value;
                    if (clazz == String.class) {
                        value = rawValue == null ? null : (T) String.valueOf(rawValue);
                    } else if (Number.class.isAssignableFrom(clazz) && rawValue instanceof Number) {
                        value = (T) Utils.convertNumberToTargetClass((Number) rawValue, (Class) clazz);
                    } else {
                        //暂时不进行类型判断 直接转换
                        value = (T) rawValue;
                    }

                    results.add(new TimedResult<>(columnName,time, value));
                });
            });
            return results;
        });
    }

    public <T> Optional<T> queryForObject(QueryCreator creator, QueryResultMapper<T> resultMapper) {
        return execute(client -> {

            QueryResult queryResult = client.query(creator.createQuery());

            if (queryResult.hasError()) {
                throw new InfluxDBXException(queryResult.getError());
            }

            List<QueryResult.Result> results = queryResult.getResults();
            if (results.isEmpty()) {
                return Optional.empty();
            }

            return Optional.ofNullable(resultMapper.call(queryResult));
        });
    }

    public <T> Optional<T> queryForObject(QueryCreator creator, QueryResultSeriesMapper<T> seriesMapper) {
        return Utils.first(queryForList(creator, seriesMapper));
    }

    public <T> List<T> queryForList(QueryCreator creator, QueryResultSeriesMapper<T> seriesMapper) {

        return execute(client -> {

            QueryResult queryResult = client.query(creator.createQuery(), TimeUnit.MILLISECONDS);

            if (queryResult.hasError() || queryResult.getError() != null) {
                throw new InfluxDBXException("InfluxDB returned an error: " + queryResult.getError());
            }

            List<QueryResult.Result> results = queryResult.getResults();

            if (results.isEmpty()) {
                return Collections.emptyList();
            }

            queryResult.getResults().forEach(seriesResult -> {
                if (seriesResult.getError() != null) {
                    throw new InfluxDBXException("InfluxDB returned an error with Series: " + seriesResult.getError());
                }
            });

            List<T> list = new ArrayList<>(results.size());

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> list.add(seriesMapper.call(series)));

            return list;
        });
    }

    private InfluxDB getResource() {
        try {
            return dataSource.borrowObject();
        } catch (NoSuchElementException nse) {
            throw new InfluxDBXException("Could not get a resource from the pool", nse);
        } catch (Exception e) {
            throw new InfluxDBXConnectionException("Could not get a resource from the pool", e);
        }
    }

    private <T> List<T> queryForListByQuery(String database, String measurement, String rawQuery, Map<String, Object> argsMap, InfluxDBXMapper<T> mapper) {
        return execute(client -> {
            //因为query中的measurement不能使用bind 这里先手动替换 - 暂时固定名称为 measurement

            String queryString = Utils.isNotEmpty(measurement) ?
                    rawQuery.replaceAll("#measurement#", measurement) : rawQuery;

            //如果没有指定timezone的话 这里补充一个当前时区到查询中

            if (!queryString.toLowerCase().contains("tz(")) {
                String tz = " tz('" + zoneId.toString() + "') ";
                //如果有多条语句 为各个语句都加上tz
                if(queryString.contains(";")) {
                    queryString = Arrays.stream(queryString.split(";"))
                            .filter(str -> !str.trim().isEmpty())
                            .map(str -> str + tz)
                            .collect(Collectors.joining(";"));
                }else {
                    queryString = queryString + tz;
                }
            }

            BoundParameterQuery.QueryBuilder queryBuilder =
                    BoundParameterQuery.QueryBuilder.newQuery(queryString).forDatabase(database);

            if (argsMap != null && !argsMap.isEmpty()) {
                argsMap.forEach(queryBuilder::bind);
            }

            BoundParameterQuery boundParameterQuery = queryBuilder.create();

            QueryResult queryResult = client.query(boundParameterQuery, TimeUnit.MILLISECONDS);

            return mapper.mapResult(queryResult);
        });
    }

    private Instant convertTimeValue(Object value, TimeUnit precision) {
        Instant instant;
        if (value instanceof String) {
            instant = Instant.from(RFC3339_FORMATTER.parse(String.valueOf(value)));
        } else if (value instanceof Long) {
            instant = Instant.ofEpochMilli(toMillis((long) value, precision));
        } else if (value instanceof Double) {
            instant = Instant.ofEpochMilli(toMillis(((Double) value).longValue(), precision));
        } else if (value instanceof Integer) {
            instant = Instant.ofEpochMilli(toMillis(((Integer) value).longValue(), precision));
        } else {
            throw new InfluxDBXException("Unsupported type " + value.getClass() + " convert to Instant");
        }
        return instant;
    }

    private Long toMillis(final long value, final TimeUnit precision) {
        return TimeUnit.MILLISECONDS.convert(value, precision);
    }


    public @Nonnull Map<String, Object> queryForMapByQuery(String measurement, String queryString, Map<String, Object> paramMap) {
        return queryForMapByQuery(null, measurement, queryString, paramMap);
    }

    public @Nonnull Map<String, Object> queryForMapByQuery(String database,String measurement, String queryString, Map<String, Object> paramMap) {
        List<Map<String, Object>> mapList = queryForMapListByQuery(database,measurement, queryString, paramMap);
        return Utils.first(mapList).orElse(Collections.emptyMap());
    }

    public List<Map<String, Object>> queryForMapListByQuery(String database,String measurement, String queryString, Map<String, Object> paramMap) {

        return queryForListByQuery(database, measurement, queryString, paramMap, queryResult -> {

            List<Map<String, Object>> resultMap = new ArrayList<>(queryResult.getResults().size());

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {
                List<String> columns = series.getColumns();

                series.getValues().forEach(values -> {

                    Map<String, Object> columnMap = new HashMap<>();

                    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
                        columnMap.put(columns.get(columnIndex), values.get(columnIndex));
                        resultMap.add(columnMap);
                    }
                });


            });
            return resultMap;
        });
    }
}
