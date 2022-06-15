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
public class Event0 {
    private int id;

    private String name;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event0 event0 = (Event0) o;
        return id == event0.id && Objects.equals(name, event0.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
