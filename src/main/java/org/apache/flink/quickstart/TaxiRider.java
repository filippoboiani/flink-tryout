package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by filippoboiani on 11/08/2017.
 */
public class TaxiRider {

    public static void main(String[] args) throws Exception {

        // get an ExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure event-time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the degree of parallelism
        // env.setParallelism(1);

        final int popThreshold = 20;        // after 20 you are popular
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
        final Path pathToTheFile = Paths.get("/Users/filippoboiani/flink-java-project/src/main/resources/nycTaxiRides.gz");

        // get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(pathToTheFile.toString(), maxEventDelay, servingSpeedFactor));

        // DataStream<TaxiRide> filteredRides = rides.filter(new TaxiFilter());

        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> results = rides
                // filter the results
                .filter(new TaxiFilter())
                // match ride to grid cell and event type (start or end)
                .map(new GridCellMatcher())
                // partition by cell id and event type
                .<KeyedStream<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>>keyBy(0, 1)
                // build sliding window
                .timeWindow(Time.minutes(15), Time.minutes(5))
                // count ride events in window
                .apply(new RideCounter())
                // filter by popularity threshold
                .filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
                        return count.f3 >= popThreshold;
                    }
                })
                // map grid cell to coordinates
                .map(new GridToCoordinates());

        // print the filtered stream
        results.print();

        // run the cleansing pipeline
        env.execute("Popular Places");
    }

    public static class TaxiFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

    /**
     * Map taxi ride to grid cell and event type.
     * Start records use departure location, end record use arrival location.
     */
    public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
            if(taxiRide.isStart) {
                // get grid cell id for start location
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                return new Tuple2<>(gridId, true);
            } else {
                // get grid cell id for end location
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                return new Tuple2<>(gridId, false);
            }
        }
    }

    /**
     * Counts the number of rides arriving or departing.
     */
    public static class RideCounter implements WindowFunction<
                Tuple2<Integer, Boolean>, // input type
                Tuple4<Integer, Long, Boolean, Integer>, // output type
                Tuple, // key type
                TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<Integer, Boolean>> values,
                Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

            int cellId = ((Tuple2<Integer, Boolean>)key).f0;
            boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
            long windowTime = window.getEnd();

            int cnt = 0;
            for(Tuple2<Integer, Boolean> v : values) {
                cnt += 1;
            }

            out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
        }
    }

    /**
     * Maps the grid cell id back to longitude and latitude coordinates.
     */
    public static class GridToCoordinates implements
            MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

        @Override
        public Tuple5<Float, Float, Long, Boolean, Integer> map(
                Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

            return new Tuple5<>(
                    GeoUtils.getGridCellCenterLon(cellCount.f0),
                    GeoUtils.getGridCellCenterLat(cellCount.f0),
                    cellCount.f1,
                    cellCount.f2,
                    cellCount.f3);
        }
    }


}
