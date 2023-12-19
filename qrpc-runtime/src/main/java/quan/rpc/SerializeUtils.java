package quan.rpc;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.mutable.MutableObject;
import quan.rpc.Protocol.Handshake;
import quan.rpc.Protocol.PingPong;
import quan.rpc.Protocol.Request;
import quan.rpc.Protocol.Response;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 序列化工具
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SerializeUtils {

    private static final Map<Integer, Class<?>> id2Classes = new HashMap<>();

    private static final Map<Class<?>, Integer> class2Ids = new HashMap<>();

    private static final ThreadLocal<LinkedBuffer> localBuffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(2048));

    static {
        registerRoot(0, MutableObject.class);
        registerRoot(1, Request.class);
        registerRoot(2, Response.class);
        registerRoot(3, PingPong.class);
        registerRoot(4, Handshake.class);
    }

    public static void registerRoot(int id, Class<?> clazz) {
        if (id2Classes.containsKey(id)) {
            throw new IllegalStateException("不能重复注册:" + id);
        } else if (class2Ids.containsKey(Objects.requireNonNull(clazz))) {
            throw new IllegalStateException("不能重复注册:" + clazz);
        } else {
            id2Classes.put(id, clazz);
            class2Ids.put(clazz, id);
        }
    }

    private static Schema<Object> getSchema(Integer id) {
        return (Schema<Object>) RuntimeSchema.getSchema(id2Classes.get(id));
    }

    private static LinkedBuffer allocBuffer(boolean useLocalBuffer) {
        if (useLocalBuffer) {
            return localBuffer.get();
        } else {
            return LinkedBuffer.allocate();
        }
    }

    private static boolean useLocalBuffer() {
        Worker worker = Worker.current();
        return worker != null && !(worker instanceof ThreadPoolWorker);
    }

    public static long readVarInt64(InputStream is) {
        long temp = 0;
        int shift = 0;
        int count = 0;

        try {
            while (count < 10) {
                final byte b = (byte) is.read();
                if (b == -1) {
                    break;
                }
                temp |= (b & 0x7FL) << shift;
                shift += 7;
                count++;

                if ((b & 0x80) == 0) {
                    return (temp >>> 1) ^ -(temp & 1);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("readVarInt64 error", e);
        }

        throw new RuntimeException("readVarInt64 error");
    }

    public static void writeVarInt64(OutputStream os, long n) {
        n = (n << 1) ^ (n >> 63);

        try {
            while (true) {
                if ((n & ~0x7F) == 0) {
                    os.write((byte) (n & 0x7F));
                    return;
                } else {
                    os.write((byte) (n & 0x7F | 0x80));
                    n >>>= 7;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("writeVarInt64 error", e);
        }
    }

    /**
     * 序列化对象
     */
    public static byte[] serialize(Object obj) {
        return serialize(obj, useLocalBuffer());
    }

    static byte[] serialize(Object obj, boolean useLocalBuffer) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        serialize(obj, os, useLocalBuffer);
        return os.toByteArray();
    }

    /**
     * 序列化对象
     */
    public static void serialize(Object obj, OutputStream os) {
        serialize(obj, os, useLocalBuffer());
    }

    static void serialize(Object obj, OutputStream os, boolean useLocalBuffer) {
        if (obj == null || obj instanceof MutableObject) {
            throw new IllegalArgumentException("不支持序列化:" + obj);
        }

        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            Integer id = class2Ids.getOrDefault(obj.getClass(), 0);
            Schema<Object> schema = getSchema(id);

            writeVarInt64(os, id);

            Object root = id == 0 ? new MutableObject<>(obj) : obj;
            ProtostuffIOUtil.writeTo(os, root, schema, buffer);
        } catch (Exception e) {
            throw new SerializationException(e);
        } finally {
            buffer.clear();
        }
    }


    /**
     * 反序列化对象
     */
    public static <T> T deserialize(byte[] bytes) {
        return deserialize(new ByteArrayInputStream(bytes), useLocalBuffer());

    }

    /**
     * 反序列化对象
     */
    public static <T> T deserialize(InputStream is) {
        return deserialize(is, useLocalBuffer());
    }

    static <T> T deserialize(InputStream is, boolean useLocalBuffer) {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            int id = (int) readVarInt64(is);

            Schema<Object> schema = getSchema(id);
            Object root = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(is, root, schema, buffer);

            if (root instanceof MutableObject) {
                return (T) ((MutableObject) root).getValue();
            } else {
                return (T) root;
            }
        } catch (Exception e) {
            throw new SerializationException(e);
        } finally {
            buffer.clear();
        }
    }

    /**
     * 复制对象
     */
    public static <T> T clone(T obj) {
        byte[] bytes = serialize(obj);
        return deserialize(bytes);
    }

    static <T> T clone(T obj, boolean useLocalBuffer) {
        byte[] bytes = serialize(obj, useLocalBuffer);
        return deserialize(new ByteArrayInputStream(bytes), useLocalBuffer);
    }

}
