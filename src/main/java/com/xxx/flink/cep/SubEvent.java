package com.xxx.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @author 0x822a5b87
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SubEvent extends Event0 {
    private double volume;

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SubEvent subEvent = (SubEvent) o;
        return Double.compare(subEvent.volume, volume) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), volume);
    }
}
