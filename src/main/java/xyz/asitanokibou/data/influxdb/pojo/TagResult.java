package xyz.asitanokibou.data.influxdb.pojo;

import lombok.*;
/**
 * @author aimysaber@gmail.com
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TagResult<T> extends TimedResult<T> {

    private String tag;
}