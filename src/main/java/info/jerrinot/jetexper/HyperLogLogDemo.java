package info.jerrinot.jetexper;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
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

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;

public class HyperLogLogDemo {
    private static final long WINDOW_SIZE = 30_000;
    private static final long SLIDING_STEP = 5_000;

    private static final DistributedSupplier<AtomicReference<ICardinality>> CARDINALITY_SUPPLIER =
            () -> new AtomicReference<>(new HyperLogLogPlus(18, 25));

    public static void main(String[] args)  {
        Pipeline pipeline = createPipeline();

        JetInstance jet = startNewInstance();
        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(AT_LEAST_ONCE) //blocked by #
                .setName("myJob");
        Job job = jet.newJobIfAbsent(pipeline, jobConfig);
        job.join();
    }

    private static Pipeline createPipeline() {
        Pipeline pipeline = Pipeline.create();
        StreamSource<Name> src = createSource();

        StageWithWindow<Name> slidingWindow = pipeline.drawFrom(src)
                .withIngestionTimestamps()
                .window(sliding(WINDOW_SIZE, SLIDING_STEP));

        StreamStageWithKey<TimestampedItem<Long>, Long> hllStreamGroupedByTimestamp = slidingWindow
                .aggregate(hllAggregationOp())
                .setName("hll aggregation")
                .groupingKey(TimestampedItem::timestamp);

        StreamStageWithKey<TimestampedItem<Long>, Long> setStreamGroupByTimestamp = slidingWindow
                .aggregate(setAggregationOp())
                .setName("set aggregation")
                .groupingKey(TimestampedItem::timestamp);

        setStreamGroupByTimestamp
                .window(tumbling(SLIDING_STEP))
                .aggregate2(hllStreamGroupedByTimestamp, cogroupOp())
                .setName("cogroup")
                .drainTo(logger(e -> toLocalTime(e.getKey()) + ", " + e.getValue()));
        return pipeline;
    }

    private static StreamSource<Name> createSource() {
        return SourceBuilder.stream("name generator", c -> new NameGenerator())
                    .<Name>fillBufferFn((s, b) -> b.add(s.generateName()))
                    .build();
    }

    private static AggregateOperation2<TimestampedItem<Long>, TimestampedItem<Long>, LongTuple, LongTuple> cogroupOp() {
        return AggregateOperation
                    .withCreate(LongTuple::new)
                    .<TimestampedItem<Long>>andAccumulate((a, i) -> a.setLeft(i.item()))
                    .<TimestampedItem<Long>>andAccumulate1((a, i) -> a.setRight(i.item()))
                    .andCombine(LongTuple::merge)
                    .andExportFinish(identity());
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
                    .withCreate(HashSet<Name>::new)
                    .<Name>andAccumulate(HashSet::add)
                    .andCombine(HashSet::addAll)
                    .andExportFinish((a) -> (long)a.size());
    }

    private static JetInstance startNewInstance() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getProperties().setProperty("hazelcast.logging.type", "slf4j");
        jetConfig.getProperties().setProperty("hazelcast.diagnostics.metric.distributed.datastructures", "true");
        jetConfig.getHazelcastConfig().getManagementCenterConfig()
                .setEnabled(true)
                .setUrl("http://localhost:8080/hazelcast-mancenter");
        jetConfig.getHazelcastConfig().getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(Name.class).setClass(NameSerializer.class)
        );
        return Jet.newJetInstance(jetConfig);
    }

}