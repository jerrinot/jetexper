package info.jerrinot.jetexper;

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.ajbrown.namemachine.Gender;
import org.ajbrown.namemachine.Name;
import org.ajbrown.namemachine.NameGenerator;

import java.io.IOException;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class Main {

    public static class NameSerializer implements StreamSerializer<Name> {
        @Override
        public void write(ObjectDataOutput out, Name object) throws IOException {
            out.writeUTF(object.getFirstName());
            out.writeUTF(object.getLastName());
            out.writeByte(object.getGender().ordinal());
        }

        @Override
        public Name read(ObjectDataInput in) throws IOException {
            String firstname = in.readUTF();
            String lastname = in.readUTF();
            Gender gender = Gender.values()[in.readByte()];
            return new Name(firstname, lastname, gender);
        }

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void destroy() {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        StreamSource<Name> src = SourceBuilder.stream("src", c -> new NameGenerator())
                .<Name>fillBufferFn((s, b) -> b.add(s.generateName()))
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(src)
                .withIngestionTimestamps()
                .window(sliding(10_000, 1_000))
                .aggregate(AggregateOperations.toSet())
                .map(e -> e.item().size())
                .drainTo(logger());

        JetInstance jet = startNewInstance();

        JobConfig jobConfig = new JobConfig()
                .setProcessingGuarantee(AT_LEAST_ONCE)
                .setName("myJob");
        Job job = jet.newJobIfAbsent(pipeline, jobConfig);

        Thread.sleep(10000);
        startNewInstance();
        job.join();
    }

    private static JetInstance startNewInstance() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(Name.class).setClass(NameSerializer.class)
        );
        return Jet.newJetInstance(jetConfig);
    }

}
