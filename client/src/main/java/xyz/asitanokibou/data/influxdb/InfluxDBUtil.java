package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;

import java.util.Collection;
import java.util.Objects;

/**
 * @author aimysaber@gmail.com
 */
public final class InfluxDBUtil {

    public static void queryResultSeriesCallback(QueryResult queryResult, QueryResultSeriesCallback callback) {

        queryResult.getResults().stream()
                .map(QueryResult.Result::getSeries)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .forEachOrdered(callback::call);
    }
}
