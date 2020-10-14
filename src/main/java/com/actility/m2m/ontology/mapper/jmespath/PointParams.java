package com.actility.m2m.ontology.mapper.jmespath;

import static com.actility.m2m.commons.service.error.DataErrorUtils.checkNotNull;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class PointParams {


    @Nullable
    public final JsonNode values;

    @Nullable
    public final JsonNode eventTime;

    @Nonnull
    public final JsonNode longitude;

    @Nonnull
    public final JsonNode latitude;

    @Nonnull
    public final JsonNode altitude;

    @Nonnull
    public final boolean isAltitude;

    @Nonnull
    public final boolean isValue;

    @Nonnull
    public final boolean isCoordinate;

    protected PointParams(Builder builder) {


        values = builder.values;

        eventTime = builder.eventTime;

        longitude = builder.longitude;

        latitude = builder.latitude;

        altitude = builder.altitude;

        isAltitude = builder.isAltitude;

        isValue = builder.isValue;

        isCoordinate = builder.isCoordinate;

    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)


                .add("values", values)

                .add("eventTime", eventTime)

                .add("longitude", longitude)

                .add("latitude", latitude)

                .add("altitude", altitude)

                .add("isAltitude", isAltitude)

                .add("isValue", isValue)

                .add("isCoordinate", isCoordinate)

                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                values,

                eventTime,

                longitude,

                latitude,

                altitude,

                isAltitude,

                isValue,

                isCoordinate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PointParams other = (PointParams) o;

        return Objects.equal(values, other.values)
                && Objects.equal(eventTime, other.eventTime)
                && Objects.equal(longitude, other.longitude)
                && Objects.equal(latitude, other.latitude)
                && Objects.equal(altitude, other.altitude)
                && Objects.equal(isAltitude, other.isAltitude)
                && Objects.equal(isValue, other.isValue)
                && Objects.equal(isCoordinate, other.isCoordinate);

    }

    public static Builder newPointParamsBuilder() {
        return new Builder();
    }

    public static Builder newPointParamsBuilder(PointParams other) {
        return new Builder(other);
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {

        @Nullable
        private JsonNode values;

        @Nullable
        private JsonNode eventTime;

        @Nonnull
        private JsonNode longitude;

        @Nonnull
        private JsonNode latitude;

        @Nonnull
        private JsonNode altitude;

        @Nonnull
        private boolean isAltitude;

        @Nonnull
        private boolean isValue;

        @Nonnull
        private boolean isCoordinate;


        private Builder() {

        }

        private Builder(PointParams other) {

            values = other.values;

            eventTime = other.eventTime;

            longitude = other.longitude;

            latitude = other.latitude;

            altitude = other.altitude;

            isAltitude = other.isAltitude;

            isValue = other.isValue;

            isCoordinate = other.isCoordinate;

        }


        @Nonnull
        public Builder values(@Nullable JsonNode val) {
            values = val;
            return this;
        }


        @Nonnull
        public Builder eventTime(@Nullable JsonNode val) {
            eventTime = val;
            return this;
        }


        @Nonnull
        public Builder longitude(@Nullable JsonNode val) {
            longitude = val;
            return this;
        }

        @Nonnull
        public Builder latitude(@Nullable JsonNode val) {
            latitude = val;
            return this;
        }


        @Nonnull
        public Builder altitude(@Nullable JsonNode val) {
            altitude = val;
            return this;
        }


        @Nonnull
        public Builder isAltitude(@Nullable boolean val) {
            isAltitude = val;
            return this;
        }

        @Nonnull
        public Builder isValue(@Nullable boolean val) {
            isValue = val;
            return this;
        }


        @Nonnull
        public Builder isCoordinate(@Nullable boolean val) {
            isCoordinate = val;
            return this;
        }


        @Nonnull
        public PointParams build() {
            return new PointParams(this);
        }

    }


}
