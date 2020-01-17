package com.example.flink;

import cloudflow.flink.FlinkStreamlet;
import cloudflow.flink.FlinkStreamletLogic;
import cloudflow.streamlets.StreamletShape;
import cloudflow.streamlets.avro.AvroInlet;
import cloudflow.streamlets.avro.AvroOutlet;
import com.ey.model.ProspectEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FlinkProcessorWithTimeWindow extends FlinkStreamlet {

    AvroInlet<ProspectEvent> in = AvroInlet.<ProspectEvent>create("in", ProspectEvent.class);
    AvroOutlet<ProspectEvent> out = AvroOutlet.<ProspectEvent>create("out", (ProspectEvent s) -> s.getCustomerId(), ProspectEvent.class);

    // Step 2: Define the shape of the streamlet. In this example the streamlet
    //         has 1 inlet and 1 outlet
    @Override public StreamletShape shape() {
        return StreamletShape.createWithInlets(in).withOutlets(out);
    }

    // Step 3: Provide custom implementation of `FlinkStreamletLogic` that defines
    //         the behavior of the streamlet
    @Override public FlinkStreamletLogic createLogic() {
        return new FlinkStreamletLogic(getContext()) {
            @Override public void buildExecutionGraph() {

                DataStream<ProspectEvent> ins =
                        this.<ProspectEvent>readStream(in, ProspectEvent.class)
                                .map((ProspectEvent d) -> d)
                                .returns(new TypeHint<ProspectEvent>(){}.getTypeInfo());

                DataStream<ProspectEvent> simples = ins
                        .map((ProspectEvent pe) -> new Tuple2(pe.getCustomerId(), pe))
                        .keyBy(0)
                        .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10)))
                        .process(new CustomCountFunction(5));
                DataStreamSink<ProspectEvent> sink = writeStream(out, simples, ProspectEvent.class);
            }
        };
    }

    public class ProspectCount {

        public ProspectEvent message;
        public Integer count;

        public ProspectCount(ProspectEvent message, Integer count) {
            this.message = message;
            this.count = count;
        }
    }

    public class CustomCountFunction extends ProcessWindowFunction<Tuple2, ProspectEvent,Tuple, TimeWindow> {

        private Integer count;
        private ValueState<ProspectCount> state;

        public CustomCountFunction(Integer count) {
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", ProspectCount.class));
        }

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2> elements, Collector<ProspectEvent> out) throws Exception {
            if (Iterables.size(elements) > count) {
                out.collect((ProspectEvent) Iterables.getLast(elements).f0);
            }
        }
    }
}


