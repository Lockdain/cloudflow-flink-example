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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class FlinkProcessor extends FlinkStreamlet {

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
                        .process(new CustomCountFunction(5, TimeUnit.DAYS.toMillis(1)));
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

    public class CustomCountFunction extends KeyedProcessFunction<Tuple, Tuple2, ProspectEvent> {

        private Integer count;
        private ValueState<ProspectCount> state;
        private Long window;

        public CustomCountFunction(Integer count, Long window) {
            this.count = count;
            this.window = window;
        }

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", ProspectCount.class));
        }

        @Override
        public void processElement(Tuple2 value, Context ctx, Collector<ProspectEvent> out) throws Exception {

            ProspectCount current = (ProspectCount) state.value();

            if (current == null) {
                current = new ProspectCount((ProspectEvent) value.f0, 0);
                ctx.timerService().registerProcessingTimeTimer(window);
            }

            current.count++;

            state.update(current);

            if (current.count > count) {
                out.collect((ProspectEvent) value.f0);
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<ProspectEvent> out) throws Exception {

            state.update(null);
        }
    }
}


