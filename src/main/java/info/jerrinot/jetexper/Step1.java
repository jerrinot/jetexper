package info.jerrinot.jetexper;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.*;
import com.hazelcast.jet.pipeline.*;
import org.ajbrown.namemachine.*;


public class Step1 {
    public static void main(String[] args) {
        JetInstance jet = Jet.newJetInstance();

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
