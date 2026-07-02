package com.manaschintawar.kvstore.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A value with a last-write-wins timestamp. Deletes are recorded as tombstones
 * so that a replica that missed a delete cannot resurrect the old value.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VersionedValue {
    private String value;
    private long timestamp;
    private boolean tombstone;

    public VersionedValue() {}

    public VersionedValue(String value, long timestamp, boolean tombstone) {
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public boolean isTombstone() { return tombstone; }
    public void setTombstone(boolean tombstone) { this.tombstone = tombstone; }

    /** True if this record is strictly newer than {@code other} (null = missing). */
    public boolean isNewerThan(VersionedValue other) {
        return other == null || this.timestamp > other.getTimestamp();
    }
}
