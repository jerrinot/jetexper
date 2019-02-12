package info.jerrinot.jetexper;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import org.ajbrown.namemachine.Name;
import org.ajbrown.namemachine.NameGenerator;
import org.ajbrown.namemachine.NameMachine;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;

public class HyperLogLogDemo {
    private static final long WINDOW_SIZE = 15_000;
    private static final long SLIDING_STEP = 1_000;

    private static final Name NAME = new NameGenerator().generateName();

    private static final int HYPERLOGLOG_PRECISION = 10;
    private static final DistributedSupplier<AtomicReference<ICardinality>> CARDINALITY_SUPPLIER =
            () -> new AtomicReference<>(new HyperLogLogPlus(HYPERLOGLOG_PRECISION));

    public static void main(String[] args)  {
        JetInstance jet = startNewInstance();

        JobConfig jobConfig = new JobConfig().setName("hyperloglog demo");
        Pipeline pipeline = createPipeline();

        jet.newJobIfAbsent(pipeline, jobConfig)
                .join();
    }

    private static Pipeline createPipeline() {
        Pipeline pipeline = Pipeline.create();
        StreamSource<Name> source = randomNamesSource();

        StageWithWindow<Name> slidingWindow = pipeline.drawFrom(source)
                .withIngestionTimestamps()
                .window(sliding(WINDOW_SIZE, SLIDING_STEP));

        StreamStageWithKey<TimestampedItem<Long>, Long> hllStreamGroupedByTimestamp = slidingWindow
                .aggregate(hllAggregationOp())
                .setName("hll aggregation")
                .groupingKey(TimestampedItem::timestamp);

        StreamStageWithKey<TimestampedItem<Long>, Long> setStreamGroupedByTimestamp = slidingWindow
                .aggregate(hllAggregationOp())
                .setName("hll aggregation")
                .groupingKey(TimestampedItem::timestamp);

//        StreamStageWithKey<TimestampedItem<Long>, Long> setStreamGroupedByTimestamp = slidingWindow
//                .aggregate(setAggregationOp())
//                .setName("set aggregation")
//                .groupingKey(TimestampedItem::timestamp);

        StreamStageWithKey<TimestampedItem<Long>, Long> countStreamGroupedByTimestamp = slidingWindow
                .aggregate(AggregateOperations.counting())
                .setName("count aggregation")
                .groupingKey(TimestampedItem::timestamp);

        hllStreamGroupedByTimestamp
                .window(tumbling(SLIDING_STEP))
                .aggregate3(setStreamGroupedByTimestamp, countStreamGroupedByTimestamp, joinOp())
                .setName("stream join")
                .drainTo(logger(e -> toLocalTime(e.getKey()) + ", " + e.getValue()));
        return pipeline;
    }

    private static StreamSource<Name> randomNamesSource() {
        return SourceBuilder.stream("name generator", c -> new NameGenerator())
//                    .<Name>fillBufferFn((s, b) -> b.add(s.generateName()))
                .<Name>fillBufferFn((s, b) -> b.add(NAME))
                    .build();
    }

    private static AggregateOperation3<TimestampedItem<Long>, TimestampedItem<Long>, TimestampedItem<Long>, ?, LongTripple> joinOp() {
        return AggregateOperation
                    .withCreate(LongTripple::new)
                    .<TimestampedItem<Long>>andAccumulate0((a, i) -> a.setLeft(i.item()))
                    .<TimestampedItem<Long>>andAccumulate1((a, i) -> a.setRight(i.item()))
                    .<TimestampedItem<Long>>andAccumulate2((a, i) -> a.setMiddle(i.item()))
                    .andCombine(LongTripple::merge)
                    .andExportFinish(LongTripple::new);
    }

    private static AggregateOperation1<Name, ?, Long> hllAggregationOp() {
        return AggregateOperation
                    .withCreate(CARDINALITY_SUPPLIER)
                    .<Name>andAccumulate((hll,name) -> hll.get().offer(name))
                    .andCombine((a, b) -> a.set(a.get().merge(b.get())))
                    .andExportFinish(hll -> hll.get().cardinality());
    }

    private static AggregateOperation1<Name, HashSet<Name>, Long> setAggregationOp() {
        return AggregateOperation
                    .withCreate(() -> new HashSet<Name>(8_000_000))
                    .<Name>andAccumulate(HashSet::add)
                    .andCombine(HashSet::addAll)
                    .andExportFinish((a) -> (long)a.size());
    }

    private static JetInstance startNewInstance() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getProperties().setProperty("hazelcast.logging.type", "slf4j");
        jetConfig.getHazelcastConfig().getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(Name.class).setClass(NameSerializer.class)
        );
        return Jet.newJetInstance(jetConfig);
    }

}
