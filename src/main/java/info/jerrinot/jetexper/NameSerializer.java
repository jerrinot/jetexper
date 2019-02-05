package info.jerrinot.jetexper;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.ajbrown.namemachine.Gender;
import org.ajbrown.namemachine.Name;

import java.io.IOException;

public final class NameSerializer implements StreamSerializer<Name> {
    private static final int ID = 1;

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
        return ID;
    }

    @Override
    public void destroy() {

    }
}
