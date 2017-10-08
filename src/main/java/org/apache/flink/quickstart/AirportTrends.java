package org.apache.flink.quickstart;

/**
 * Airport Trends
 *
 *
 * Created by filippoboiani on 06/10/2017.
 */
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.*;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.TimeZone;



public class AirportTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }
    }

    public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple3<Integer, JFKTerminal, Boolean>> {

        @Override
        public Tuple3<Integer, JFKTerminal, Boolean> map(TaxiRide taxiRide) throws Exception {
            if(taxiRide.isStart) {
                // get grid cell id for start location
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple3<>(gridId, terminal, true);
            } else {
                // get grid cell id for end location
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple3<>(gridId, terminal, false);
            }
        }
    }

    public static class TerminalFilter implements FilterFunction<Tuple3<Integer, JFKTerminal, Boolean>> {
        @Override
        public boolean filter(Tuple3<Integer, JFKTerminal, Boolean> taxiRide) throws Exception {

            return taxiRide.f1 != JFKTerminal.NOT_A_TERMINAL;
        }
    }



    public static void main(String[] args) throws Exception {

        // Create and set the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final int maxDelayInSecs = 60;
        final int servingSpeedFactor = 2000;

        // get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("/Users/filippoboiani/flink-java-project/src/main/resources/nycTaxiRides.gz", maxDelayInSecs, servingSpeedFactor));

        // Task 1 & 2
        rides
                // match ride to grid cell, event type (start or end) and TERMINAL
                .map(new GridCellMatcher())
                // get only the rides starting from or ending to the terminal
                .filter(new TerminalFilter())
                .<KeyedStream<Tuple3<Integer, JFKTerminal, Boolean>, JFKTerminal>>keyBy(1)
                // build sliding window
                .timeWindow(Time.minutes(60), Time.minutes(60))
                // count the number of rides for each terminal
                .apply(new RideCounter())
                .<KeyedStream<Tuple3<JFKTerminal, Integer, Integer>, Integer>>keyBy(2)
                .timeWindowAll(Time.minutes(60), Time.minutes(60))
                .reduce(new BestTerminal())
                .print();


        env.execute("Airport Trends");

    }

    private static class RideCounter implements WindowFunction<
            Tuple3<Integer, JFKTerminal, Boolean>,
            Tuple3<JFKTerminal, Integer, Integer>,
            Tuple,
            TimeWindow> {
        @Override
        public void apply(
                Tuple key, // key
                TimeWindow timeWindow,
                Iterable<Tuple3<Integer, JFKTerminal, Boolean>> values,
                Collector<Tuple3<JFKTerminal, Integer, Integer>> collector) throws Exception {

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone( "America/New_York" ));
            calendar.setTimeInMillis(timeWindow.getEnd());

            JFKTerminal terminal = key.getField(0);
            int cnt = 0;
            for(Tuple3<Integer, JFKTerminal, Boolean> v : values) {
                cnt += 1;
            }

            collector.collect(new Tuple3<>(terminal, cnt, calendar.get(Calendar.HOUR_OF_DAY)));
        }
    }


    private static class BestTerminal implements ReduceFunction<Tuple3<JFKTerminal, Integer, Integer>> {
        @Override
        public Tuple3<JFKTerminal, Integer, Integer> reduce(
                Tuple3<JFKTerminal, Integer, Integer> ride0,
                Tuple3<JFKTerminal, Integer, Integer> ride1) throws Exception {

            if (ride0.f1 > ride1.f1) {
                return ride0;
            } else {
                return ride1;
            }

        }
    }
}