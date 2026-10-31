package io.github.enterprise.kafka.serialization;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class JsonCodecTest { record Event(String id) {} @Test void roundTrip(){JsonCodec codec=new JsonCodec(); assertEquals(new Event("a"),codec.decode(codec.encode(new Event("a")),Event.class));} @Test void malformed(){assertThrows(SerializationException.class,()->new JsonCodec().decode(new byte[]{1},Event.class));} }
