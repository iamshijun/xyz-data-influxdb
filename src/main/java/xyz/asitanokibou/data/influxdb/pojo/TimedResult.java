package xyz.asitanokibou.data.influxdb.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
/**
 * @author aimysaber@gmail.com
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TimedResult<T> {

    private String name; //columnName;
    private Instant time;
    private T value;

    public LocalDateTime getLocalTime(){
        return LocalDateTime.ofInstant(time, ZoneId.systemDefault());
    }
}
