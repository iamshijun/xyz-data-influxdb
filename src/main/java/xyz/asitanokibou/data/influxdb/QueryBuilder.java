package xyz.asitanokibou.data.influxdb;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.influxdb.dto.BoundParameterQuery;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * @author aimysaber@gmail.com
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryBuilder {

    private String database;
    private String measurement;

    private Map<String, Object> fields = new HashMap<>();
    private Map<String, AtomicInteger> sameFieldCondCount = new HashMap<>();
    //todo add nested
    private StringBuilder nestQueryBuilder = new StringBuilder();

    private StringBuilder conditionBuilder = new StringBuilder();
    //private StringBuilder projection = new StringBuilder();
    private String projection = "*";
    private String timezone = "tz('Asia/Shanghai')";

    public static QueryBuilder create() {
        return new QueryBuilder();
    }

    public BoundParameterQuery toQuery() {

        BoundParameterQuery.QueryBuilder queryBuilder =
                BoundParameterQuery.QueryBuilder.newQuery(
                        "select " + projection + " from " + measurement + " " + conditionBuilder.toString() + " " + timezone);
        queryBuilder.forDatabase(database);
        fields.forEach(queryBuilder::bind);

        return queryBuilder.create();

    }

    /*public BoundParameterQuery toNestQuery() {
        BoundParameterQuery.QueryBuilder queryBuilder =
                BoundParameterQuery.QueryBuilder.newQuery(
                        "select " + projection + " from " + measurement + " " + conditionBuilder.toString() + " " );
        fields.forEach(queryBuilder::bind);

        return queryBuilder.create();
    }*/

    private void append(String fieldName, Operator operator, Object... values) {

        if (fields.isEmpty()) {
            conditionBuilder.append(" where ");
        } else {
            conditionBuilder.append(" and ");
        }

        AtomicInteger atomicInteger = sameFieldCondCount.computeIfAbsent(fieldName, k -> new AtomicInteger(0));

        String propertyName = fieldName + "_" + atomicInteger.incrementAndGet();
        fields.put(propertyName, values[0]);

        String placeHolderName = "$" + propertyName;

        switch (operator) {
            case EQ:
                conditionBuilder.append(fieldName).append("=").append(placeHolderName);
                break;
            case NE:
                conditionBuilder.append(fieldName).append("!=").append(placeHolderName);
                break;
            case LT:
                conditionBuilder.append(fieldName).append("<").append(placeHolderName);
                break;
            case GT:
                conditionBuilder.append(fieldName).append(">").append(placeHolderName);
                break;
            case GE:
                conditionBuilder.append(fieldName).append(">=").append(placeHolderName);
                break;
            case LE:
                conditionBuilder.append(fieldName).append("<=").append(placeHolderName);
                break;
            case BETWEEN:
                Object appendValue1, appendValue2;

                String startFieldName = fieldName + "_start";
                String endFieldName = fieldName + "_end";

                //fields.remove(fieldName);
                fields.put(startFieldName, values[0]);
                fields.put(endFieldName, values[1]);

                appendValue1 = "$" + startFieldName;
                appendValue2 = "$" + endFieldName;


                conditionBuilder.append(fieldName).append(">=").append(appendValue1)
                        .append(fieldName).append("<=").append(appendValue2);
                break;
            case IN:
                break;
        }
    }

    public QueryBuilder measurement(String measurement) {
        this.measurement = measurement;
        return this;
    }

    public QueryBuilder database(String database) {
        this.database = database;
        return this;
    }

    public QueryBuilder projection(String projection) {
        this.projection = projection;
        return this;
    }

    public QueryBuilder timezone(String timezone) {
        this.timezone = timezone;
        return this;
    }

    public QueryBuilder equals(String field, Object value) {
        value = preProcessValue(value);
        append(field, Operator.EQ, value);
        return this;

    }

    public QueryBuilder greaterThan(String field, Object value) {
        value = preProcessValue(value);
        append(field, Operator.GT, value);
        return this;
    }

    public QueryBuilder lessThan(String field, Object value) {
        value = preProcessValue(value);
        append(field, Operator.LT, value);
        return this;
    }


    public QueryBuilder greaterEquals(String field, Object value) {
        value = preProcessValue(value);
        append(field, Operator.GE, value);
        return this;
    }

    private Object preProcessValue(Object value) {
        if (value instanceof LocalDateTime) {
            value = ((LocalDateTime) value).format(Utils.YMDHMS_FMT);
        }
        return value;
    }

    public QueryBuilder lessEquals(String field, Object value) {
        value = preProcessValue(value);
        append(field, Operator.LE, value);
        return this;
    }


    public QueryBuilder between(String field, Object start, Object end) {
        append(field, Operator.BETWEEN, start, end);
        return this;
    }

    public QueryBuilder timeBetween(String field, LocalDateTime start, LocalDateTime end) {
        return timeBetween(field, start, end, Utils.YMDHMS_PATTERN);
    }

    public QueryBuilder timeBetween(String field, LocalDateTime start, LocalDateTime end, String pattern) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        append(field, Operator.BETWEEN, start.format(dateTimeFormatter), end.format(dateTimeFormatter));
        return this;
    }

//    public void appendTo(StringBuilder sb) {
//        sb.append(queryBuilder.toString());
//    }
//
//    public void doPrepaend(String prefixSqlSlice) {
//
//    }

    enum Operator {
        EQ, NE, GT, LT, GE, LE, IN, NOTIN, BETWEEN
    }
}
