package ru.mail.polis.shah;

import java.util.Arrays;

public class ByteArrayWrapper {
    private final byte[] data;
    private final int hashcode;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
        this.hashcode = Arrays.hashCode(data);
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ByteArrayWrapper) {
            ByteArrayWrapper other = (ByteArrayWrapper) obj;
            return Arrays.equals(data, other.data);
        } else {
            return false;
        }
    }
}
