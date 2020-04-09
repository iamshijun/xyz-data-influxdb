package xyz.asitanokibou.data.influxdb.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * @author aimysaber@gmail.com
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TimedResult<T> {

    private String seriesName; //measurement;
    private String name; //columnName;
//    @JsonIgnore
    private Map<String, String> tags;
    private Instant time;
    private T value;

    public LocalDateTime getLocalTime() {
        return LocalDateTime.ofInstant(time, ZoneId.systemDefault());
    }
}
