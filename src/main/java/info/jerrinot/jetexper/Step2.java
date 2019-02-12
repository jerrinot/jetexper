package info.jerrinot.jetexper;

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.ajbrown.namemachine.Name;
import org.ajbrown.namemachine.NameGenerator;


public class Step2 {
    public static void main(String[] args) {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(Name.class).setClass(NameSerializer.class));
        JetInstance jet = Jet.newJetInstance(config);

        Pipeline pipeline = buildPipeline();
        JobConfig jobConfig = new JobConfig().setName("Visitors");
        jet.newJobIfAbsent(pipeline, jobConfig);
    }

    private static Pipeline buildPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(randomNamesSource())
                .withIngestionTimestamps()
                .drainTo(Sinks.logger());
        return pipeline;
    }

    private static StreamSource<Name> randomNamesSource() {
        return SourceBuilder.stream("name generator", c -> new NameGenerator())
                .<Name>fillBufferFn((state, buffer) -> buffer.add(state.generateName()))
                .build();
    }
}
