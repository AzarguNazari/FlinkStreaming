package com.example;

import com.example.event.BankAccount;
import com.example.source.TemperatureSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


public class BankStreamingFlink {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set time characteristic to be com.example.event timestamp
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // assign fictious stream of data as our data com.example.source and extract timestamp field
        DataStream<BankAccount> inputStream = env.addSource(new TemperatureSensor()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BankAccount>() {
            @Override
            public long extractAscendingTimestamp(BankAccount sensor) {
                return sensor.getTimestamp();
            }
        });

        DataStream<Double> balances = inputStream.map(new MapFunction<BankAccount, Double>() {
            @Override
            public Double map(BankAccount sensor) throws Exception {
                return sensor.getBalance();
            }

        });

        DataStream<Double> rebalance = balances.filter(aDouble -> aDouble > 90000).rebalance();

        rebalance.print("Balance higher than 90000");


        // define a simple pattern and condition to detect from data stream
        Pattern<BankAccount, ?> highBalanced = Pattern.<BankAccount>begin("first").where(new SimpleCondition<BankAccount>() {
            @Override
            public boolean filter(BankAccount sensor) throws Exception {
                return sensor.getBalance() > 90000;
            }
        });



        // get resulted data stream from input data stream based on the defined CEP pattern
        DataStream<BankAccount> result = CEP.pattern(inputStream.keyBy(new KeySelector<BankAccount, Integer>() {
            @Override
            public Integer getKey(BankAccount sensor) throws Exception {
                return sensor.getId();
            }
        }), highBalanced).process(new PatternProcessFunction<BankAccount, BankAccount>() {
            @Override
            public void processMatch(Map<String, List<BankAccount>> map, Context context, Collector<BankAccount> collector) throws Exception {
                collector.collect(map.get("first").get(0));
            }
        });


        SingleOutputStreamOperator<Tuple3<Integer, Long, Integer>> aggregatedMatch = result.keyBy(new KeySelector<BankAccount, Integer>() {
            @Override
            public Integer getKey(BankAccount sensor) throws Exception {
                return sensor.getId();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.minutes(1))).aggregate(new AggregateFunction<BankAccount, Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>>() {
            @Override
            public Tuple3<Integer, Long, Integer> createAccumulator() {
                Tuple3<Integer, Long, Integer> acc = new Tuple3<>();
                acc.f0 = -1;
                return acc;
            }

            @Override
            public Tuple3<Integer, Long, Integer> add(BankAccount sensor, Tuple3<Integer, Long, Integer> integerLongtuple2) {
                if (integerLongtuple2.f0 == -1) {
                    integerLongtuple2.f0 = sensor.getId();
                    integerLongtuple2.f1 = sensor.getTimestamp();
                    integerLongtuple2.f2 = 0;
                }
                integerLongtuple2.f2++;
                return integerLongtuple2;
            }

            @Override
            public Tuple3<Integer, Long, Integer> getResult(Tuple3<Integer, Long, Integer> integerLongtuple2) {
                return integerLongtuple2;
            }

            @Override
            public Tuple3<Integer, Long, Integer> merge(Tuple3<Integer, Long, Integer> integerLongtuple2, Tuple3<Integer, Long, Integer> acc1) {
                acc1.f2 += integerLongtuple2.f2;
                return acc1;
            }
        });

        DataStream<Tuple2<Integer, Long>> aggregatedResult = CEP.pattern(aggregatedMatch, Pattern.<Tuple3<Integer, Long, Integer>>begin("aggs").where(new SimpleCondition<Tuple3<Integer, Long, Integer>>() {
            @Override
            public boolean filter(Tuple3<Integer, Long, Integer> integerLongTuple3) throws Exception {
                return integerLongTuple3.f2 > 4;
            }
        })).process(new PatternProcessFunction<Tuple3<Integer, Long, Integer>, Tuple2<Integer, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<Integer, Long, Integer>>> map, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                Tuple2<Integer, Long> result = new Tuple2<>();
                result.f0 = map.get("aggs").get(0).f0;
                result.f1 = map.get("aggs").get(0).f1;
                collector.collect(result);
            }
        });

//        aggregatedResult.print("Aggregated result");
//		result.print("result");
//		temps.print("temp_processed");
//		inputStream.print("input_stream");

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}