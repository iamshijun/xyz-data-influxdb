package xyz.asitanokibou.data.influxdb.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GroupByResult {

    private String name; //实际为measurement name(表);
    private Map<String, String> tags;
    private Instant time;
    private Map<String, Object> groupFieldData;

    public LocalDateTime getLocalTime() {
        return LocalDateTime.ofInstant(time, ZoneId.systemDefault());
    }
}
