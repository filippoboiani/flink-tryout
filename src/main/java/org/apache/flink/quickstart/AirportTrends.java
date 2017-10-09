package org.apache.flink.quickstart;

/**
 * Airport Trends
 *
 * Task 1
 * The first step is to generate a data stream that provides some statistics about each terminal every hour.
 * More specifically, consider only rides that start or end at an airport terminal and count the number of
 * taxi per terminal per (event-time) hour.
 *
 * Task 2
 * The airport has a single jumbo bus that can pick up people from the most popular terminal only once per hour
 * and drive them to NYC center. Extend the previous application in order to generate a stream of the  single
 * most popular terminal per hour along with the number of rides and the time of the day.
 *
 * Created by filippoboiani on 06/10/2017.
 */
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
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

    /**
     * GridCellMatcher mapper
     *
     * Map the ride to a cell and dermine wether is a ride from/to the airport.
     *
     * */
    public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple3<Integer, JFKTerminal, Boolean>> {

        @Override
        public Tuple3<Integer, JFKTerminal, Boolean> map(TaxiRide taxiRide) throws Exception {
            if(taxiRide.isStart) {
                // get grid cell id for start location
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                // get the terminal from the cell id
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple3<>(gridId, terminal, true);
            } else {
                // get grid cell id for end location
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                // get the terminal from the cell id
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple3<>(gridId, terminal, false);
            }
        }
    }

    /**
     * TerminalFilter filter
     *
     * Filter out the rides that don't have a terminal as destination or starting point.
     *
     * */
    public static class TerminalFilter implements FilterFunction<Tuple3<Integer, JFKTerminal, Boolean>> {
        @Override
        public boolean filter(Tuple3<Integer, JFKTerminal, Boolean> taxiRide) throws Exception {

            return taxiRide.f1 != JFKTerminal.NOT_A_TERMINAL;
        }
    }

    /**
     * RideCounter window function
     *
     * */
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

            // extract the hour from the ride timestamp
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone( "America/New_York" ));
            calendar.setTimeInMillis(timeWindow.getEnd());

            JFKTerminal terminal = key.getField(0);

            // count the number of rides for the terminal
            int cnt = 0;
            for(Tuple3<Integer, JFKTerminal, Boolean> v : values) {
                cnt += 1;
            }

            // output < terminal, rides count, hour >
            collector.collect(new Tuple3<>(terminal, cnt, calendar.get(Calendar.HOUR_OF_DAY)));
        }
    }

    /**
     * BusTerminal reducer
     * */
    private static class BestTerminal implements ReduceFunction<Tuple3<JFKTerminal, Integer, Integer>> {
        @Override
        public Tuple3<JFKTerminal, Integer, Integer> reduce(
                Tuple3<JFKTerminal, Integer, Integer> terminanlRides0,
                Tuple3<JFKTerminal, Integer, Integer> terminanlRides1) throws Exception {

            // compare rides count for each terminal
            if (terminanlRides0.f1 > terminanlRides1.f1) {
                return terminanlRides0;
            } else {
                return terminanlRides1;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // Create and set the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final int maxDelayInSecs = 60;
        final int servingSpeedFactor = 2000;
        final String pathToTheFile = "/Users/filippoboiani/flink-java-project/src/main/resources/nycTaxiRides.gz";

        // get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(pathToTheFile, maxDelayInSecs, servingSpeedFactor));

        // Task 1 & 2
        rides
                // match ride to grid cell, event type (start or end) and TERMINAL
                .map(new GridCellMatcher())
                // get only the rides starting from or ending to the terminal
                .filter(new TerminalFilter())
                // key by TERMINAL
                .<KeyedStream<Tuple3<Integer, JFKTerminal, Boolean>, JFKTerminal>>keyBy(1)
                // build a 1 hour sliding window with 1 hour time size and 1 hour slide
                .timeWindow(Time.minutes(60), Time.minutes(60))
                // count the number of rides for each terminal (end of task 1)
                .apply(new RideCounter())
                // key by hour
                .<KeyedStream<Tuple3<JFKTerminal, Integer, Integer>, Integer>>keyBy(2)
                // build sliding window for all the streams with 1 hour time size and 1 hour slide
                .timeWindowAll(Time.minutes(60), Time.minutes(60))
                // get the terminal with the highest number of rides (end of task 2)
                .reduce(new BestTerminal())
                .print();

        /*
        * The time window is formed starting from the timestamps which is the timestamp related to
        * each taxi ride. The watermarks are hadled and assigned
        * in TaxiRideSource.
        *
        * /

        /*
            Task 1 output

            2> (TERMINAL_6,1,20)
            1> (TERMINAL_4,30,20)
            4> (TERMINAL_3,26,20)
            4> (TERMINAL_3,39,21)
            1> (TERMINAL_4,7,21)
            4> (TERMINAL_3,16,22)
            1> (TERMINAL_4,6,22)
            4> (TERMINAL_3,3,23)
            2> (TERMINAL_6,1,23)
            4> (TERMINAL_3,10,0)
            1> (TERMINAL_4,14,0)
            ...
        */

        /*
            Task 2 output

            1> (TERMINAL_4,30,20)
            2> (TERMINAL_3,39,21)
            3> (TERMINAL_3,16,22)
            4> (TERMINAL_3,3,23)
            1> (TERMINAL_4,14,0)
            2> (TERMINAL_4,45,1)
            3> (TERMINAL_4,70,2)
            4> (TERMINAL_3,48,3)
            ...
        */
        env.execute("Airport Trends");
    }
}