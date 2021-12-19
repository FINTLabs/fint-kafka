package no.fintlabs.kafka;

import java.util.Objects;

public class TestObject {

    public String string;
    public Integer integer;

    public TestObject() {
    }

    public TestObject(String string, Integer integer) {
        this.string = string;
        this.integer = integer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestObject that = (TestObject) o;
        return Objects.equals(string, that.string) && Objects.equals(integer, that.integer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string, integer);
    }
}
