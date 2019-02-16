package info.jerrinot.jetexper;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;


import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PerfTest {
    private static final long TEST_DURATION_NANOS = SECONDS.toNanos(240);
    private static final DistributedFunction<TimestampedItem<Long>, String> FORMATTING_LAMBDA = PerfTest::formatOutput;

    private static String formatOutput(TimestampedItem<Long> tsItem) {
        return "---------- "
                + toLocalTime(tsItem.timestamp())
                + ", "+ format("%,d", tsItem.item())
                + " -------";
    }


    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(dummySource())
                .withIngestionTimestamps()
                .window(tumbling(SECONDS.toMillis(10)))
                .aggregate(counting())
                .drainTo(logger(FORMATTING_LAMBDA));

        JetConfig config = new JetConfig();
        config.getProperties().setProperty("hazelcast.logging.type", "slf4j");
        JetInstance jetInstance = Jet.newJetInstance(config);
        jetInstance.newJob(pipeline).join();
        jetInstance.shutdown();
    }

    private static StreamSource<Long> dummySource() {
        return SourceBuilder.stream("dummySource", (c) -> System.nanoTime() + TEST_DURATION_NANOS)
                    .<Long>fillBufferFn((s, b) -> {
                        long now = System.currentTimeMillis();
                        if (now < s) {
                            b.add(0L);
                        } else {
                            b.close();
                        }
                    })
                    .build();
    }
}
