package io.github.enterprise.kafka.serialization;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
public final class JsonCodec {
 private final ObjectMapper mapper;
 public JsonCodec() { mapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); }
 public byte[] encode(Object value) { try { return mapper.writeValueAsBytes(value); } catch (Exception e) { throw new SerializationException("JSON encoding failed", e); } }
 public <T> T decode(byte[] bytes, Class<T> type) { try { return mapper.readValue(bytes, type); } catch (Exception e) { throw new SerializationException("JSON decoding failed", e); } }
}
