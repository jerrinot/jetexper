package info.jerrinot.jetexper;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;


import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PerfTest {
    private static final long TEST_DURATION_NANOS = SECONDS.toNanos(60);
    private static final DistributedFunction<TimestampedItem<Long>, String> FORMATTING_LAMBDA = PerfTest::formatOutput;

    private static String formatOutput(TimestampedItem<Long> tsItem) {
        return "---------- "
                + toLocalTime(tsItem.timestamp())
                + ", "+ format("%,d", tsItem.item())
                + " -------";
    }


    public static void main(String[] args) {
        StreamSource<Long> src = SourceBuilder.stream("dummySource", (c) -> System.nanoTime() + TEST_DURATION_NANOS)
                .<Long>fillBufferFn((s, b) -> {
                    long now = System.nanoTime();
                    if (now < s) {
                        IntStream.range(0, 128).forEach(i -> b.add(now));
                    } else {
                        b.close();
                    }
                })
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(src)
                .withTimestamps(e -> e, 1000)
                .window(tumbling(SECONDS.toNanos(10)))
                .aggregate(counting())
                .drainTo(logger(FORMATTING_LAMBDA));

        JetConfig config = new JetConfig();
        config.getProperties().setProperty("hazelcast.logging.type", "slf4j");
//        config.getInstanceConfig().setCooperativeThreadCount(4);
        JetInstance jetInstance = Jet.newJetInstance(config);
        jetInstance.newJob(pipeline).join();
        System.out.println("Completed");
        jetInstance.shutdown();
    }
}
