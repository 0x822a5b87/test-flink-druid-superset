package com.xxx.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author 0x822a5b87
 */
public class SimpleCep {
    public static void main(String[] args) {
        SimpleCep cep = new SimpleCep();
        cep.work();
    }

    private void work() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event0> input = env.socketTextStream("localhost", 8080)
                                      .map(message -> {

                                          return new Event0();
                                      });

        Pattern<Event0, ?> pattern = Pattern.<Event0>begin("start").where(
                new SimpleCondition<Event0>() {
                    @Override
                    public boolean filter(Event0 event) {
                        return event.getId() == 42;
                    }
                }
        ).next("middle").subtype(SubEvent.class).where(
                new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent subEvent) {
                        return subEvent.getVolume() >= 10.0;
                    }
                }
        ).followedBy("end").where(
                new SimpleCondition<Event0>() {
                    @Override
                    public boolean filter(Event0 event) {
                        return event.getName().equals("end");
                    }
                }
        );

        PatternStream<Event0> patternStream = CEP.pattern(input, pattern);

        DataStream<Alert> result = patternStream.process(
                new PatternProcessFunction<Event0, Alert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Event0>> pattern,
                            Context ctx,
                            Collector<Alert> out) throws Exception {
                        out.collect(createAlertFrom(pattern));
                    }
                });
    }

    private Alert createAlertFrom(Map<String, List<Event0>> pattern) {
        return new Alert();
    }
}
