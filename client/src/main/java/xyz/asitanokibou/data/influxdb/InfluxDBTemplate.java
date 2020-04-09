package xyz.asitanokibou.data.influxdb;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BoundParameterQuery;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import xyz.asitanokibou.data.influxdb.ex.InfluxDBXConnectionException;
import xyz.asitanokibou.data.influxdb.ex.InfluxDBXException;
import xyz.asitanokibou.data.influxdb.pojo.GroupByResult;
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
import java.util.function.Function;
import java.util.stream.Collectors;

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

    //指定时区 如果语句中没有加tz 会默认自动加上
    private ZoneId zoneId = ZoneId.of("Asia/Shanghai");     //ZoneId.systemDefault();

    public void setTimeZone(String timeZone) {
        try {
            zoneId = ZoneId.of(timeZone);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

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

    public void write(String database, String retentionPolicy,Point point){
        execute(new InfluxDBClientCallbackWithNoResult() {
            @Override
            public void executeWithNoResult(InfluxDB client) {
                //client.write(point); //保存到默认的dataase上
                //为防止写错 需要显示指定
                client.write(database, retentionPolicy, point);
            }
        });
    }

    public void writeBatch(String database, String retentionPolicy, List<Point> points){

        if (Utils.isEmpty(points)) {
            return;
        }

        execute(new InfluxDBClientCallbackWithNoResult() {
            @Override
            public void executeWithNoResult(InfluxDB client) {
                boolean batnchEnablePrev = client.isBatchEnabled();
                if (!batnchEnablePrev) {
                    client.enableBatch();
                }
                //TODO if points.size > 3000 or .... split it in multi batch process
                points.forEach(point -> client.write(database,retentionPolicy,point));
                client.flush();

                if (!batnchEnablePrev) {
                    client.disableBatch();
                }
            }
        });
    }

    /**
     * (使用默认的数据库) 自定义查询进行 列表查询
     * @param queryCreator 查询构造器
     * @param measurement 表
     * @param clazz 映射对象(含@Measurement)
     * @param <T>
     * @return
     */
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

    /**
     * 根据查询语句查询结果列表 - 用clazz(为@Measurement注解)指定映射类
     * @param database 数据库
     * @param measurement 表
     * @param queryString 查询语句
     * @param clazz 含有@Measurement注解的映射类
     * @param argsMap 参数
     * @param <T>
     * @return
     */
    public <T> List<T> queryForListByQuery(String database, String measurement, String queryString, Class<T> clazz, Map<String, Object> argsMap) {
        //TODO 添加类似jdbc的  BeanPropertyRowMapper 或者是 dbutil的 ...  bean mappers
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


    public <T> List<TimedResult<T>> queryForTimedScalarList(@Nullable String database, @Nullable String measurement, @Nonnull String queryString, Class<T> clazz, @Nullable Map<String, Object> argsMap) {
        return queryForTimedScalarList(database, measurement, queryString, clazz, argsMap, true);
    }

    public <T> List<TimedResult<T>> queryForTimedScalarList(@Nullable String database, @Nullable String measurement, @Nonnull String queryString, Class<T> clazz, @Nullable Map<String, Object> argsMap, boolean timezoneAdjust) {
        return queryForListByQuery(database, measurement, queryString, argsMap, queryResult -> {

            List<TimedResult<T>> results = new ArrayList<>();

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {

                String seriesName = series.getName();
                Map<String, String> tags = series.getTags();

                String columnName = series.getColumns().stream()
                        .filter(name -> !name.equals("time")).findFirst().orElse(null);

                List<List<Object>> values = series.getValues();
                values.forEach(valuePairs -> {
                    Instant time = convertTimeValue(valuePairs.get(0), TimeUnit.MILLISECONDS);

                    Object rawValue = valuePairs.get(1);

                    T value = convertTo(clazz, rawValue);

                    results.add(new TimedResult<>(seriesName, columnName, tags,time, value));
                });
            });
            return results;

        },timezoneAdjust);
    }

    @SuppressWarnings("unchecked")
    private <T> T convertTo(Class<T> clazz, Object rawValue) {
        T value;
        if (clazz == String.class) {
            value = rawValue == null ? null : (T) String.valueOf(rawValue);
        } else if (Number.class.isAssignableFrom(clazz) && rawValue instanceof Number) {
            value = (T) Utils.convertNumberToTargetClass((Number) rawValue, (Class) clazz);
        } else {
            //暂时不进行类型判断 直接转换
            value = (T) rawValue;
        }
        return value;
    }

    public <T> List<GroupByResult> queryForGroupByList(@Nullable String database, @Nullable String measurement,
                                                       @Nonnull String queryString, @Nullable Map<String, Object> argsMap) {
        return queryForListByQuery(database, measurement, queryString, argsMap, queryResult -> {

            List<GroupByResult> results = new ArrayList<>();

            InfluxDBUtil.queryResultSeriesCallback(queryResult, series -> {

                String theMesurement = series.getName();
                Map<String, String> tags = series.getTags();

                List<String> columnNames = series.getColumns();

                List<List<Object>> values = series.getValues();

                values.forEach(valuePairs -> {
                    Instant time = convertTimeValue(valuePairs.get(0), TimeUnit.MILLISECONDS);

                    Map<String, Object> groupFieldData = new HashMap<>();
                    for (int i = 1; i < valuePairs.size(); i++) {

                        Object rawValue = valuePairs.get(i);
                        String columnName = columnNames.get(i);

                        groupFieldData.put(columnName, rawValue);
                    }

                    results.add(new GroupByResult(theMesurement,tags,time, groupFieldData));
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
        return queryForListByQuery(database,measurement,rawQuery,argsMap,mapper,true);
    }

    /**
     * base query for List Query
     * @param database  数据库
     * @param measurement  表
     * @param rawQuery  原始查询语句
     * @param argsMap  参数map
     * @param mapper  对象映射器
     * @param timezoneAdjust 是否需要调整时区;对于 schema语句的不能添加tz在语句后
     * @param <T>
     * @return
     */
    private <T> List<T> queryForListByQuery(String database, String measurement, String rawQuery, Map<String, Object> argsMap, InfluxDBXMapper<T> mapper,boolean timezoneAdjust) {
        return execute(client -> {
            //因为query中的measurement不能使用bind 这里先手动替换 - 暂时固定名称为 measurement

            BoundParameterQuery boundParameterQuery = getBoundParameterQuery(database, measurement, rawQuery, argsMap, timezoneAdjust);

            QueryResult queryResult = client.query(boundParameterQuery, TimeUnit.MILLISECONDS);

            return mapper.mapResult(queryResult);
        });
    }

    /**
     * 构建绑定参数的query
     * @param database  数据库
     * @param measurement  表
     * @param rawQuery  原始查询语句
     * @param argsMap 参数map
     * @param timezoneAdjust - 是否需要调整时区;对于 schema语句的不能添加tz在语句后
     * @return -
     */
    BoundParameterQuery getBoundParameterQuery(String database, String measurement, String rawQuery, Map<String, Object> argsMap,boolean timezoneAdjust) {

        String queryString = Utils.isNotEmpty(measurement) ?
                rawQuery.replaceAll("#measurement#", measurement) : rawQuery;

        //如果没有指定timezone的话 这里补充一个当前时区到查询中

        //!isSchemaQueryString(queryString)
        if (timezoneAdjust && !queryString.toLowerCase().contains("tz(")) {
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

        if (Utils.isNotEmpty(argsMap)) {
            argsMap.forEach(queryBuilder::bind);
        }

        return queryBuilder.create();
    }

    private boolean isSchemaQueryString(String queryString) {
        String lowerQs = queryString.toLowerCase().trim().replaceAll("\\s{2,}"," ");
        return lowerQs.contains("show series")
                || lowerQs.contains("show measurements")
                || lowerQs.contains("show tag")
                ;
    }


    /**
     * influxdb返回的时间 不会帮我们转换成date,locatedate等类型,返回的类型可能为 long,double这里是 官方InfluxClient中的部分转换代码 将时间转换为Instant.转为LocalDate,LocalDateTime需由开发自己编写
     * @param value -
     * @param precision - 现在写死为 millseconds
     * @return - Instant
     */
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

    public @Nonnull List<Map<String, Object>> queryForMapListByQuery(String database,String measurement, String queryString, Map<String, Object> paramMap) {

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
