package quan.rpc;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 序列化工具
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SerializeUtils {

    private static final Schema<MutableObject> rootSchema = RuntimeSchema.getSchema(MutableObject.class);

    private static final ThreadLocal<LinkedBuffer> localBuffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(2048));


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

    /**
     * 序列化对象
     */
    public static <T> byte[] serialize(T obj) {
        return serialize(obj, useLocalBuffer());
    }

    static <T> byte[] serialize(T obj, boolean useLocalBuffer) {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        byte[] bytes;

        try {
            bytes = ProtostuffIOUtil.toByteArray(new MutableObject<>(obj), rootSchema, buffer);
        } finally {
            buffer.clear();
        }

        return bytes;
    }

    /**
     * 序列化对象
     */
    public static <T> void serialize(T obj, OutputStream os) throws IOException {
        serialize(obj, os, useLocalBuffer());
    }

    static <T> void serialize(T obj, OutputStream os, boolean useLocalBuffer) throws IOException {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            ProtostuffIOUtil.writeTo(os, new MutableObject<>(obj), rootSchema, buffer);
        } finally {
            buffer.clear();
        }
    }


    /**
     * 反序列化对象
     */
    public static <T> T deserialize(byte[] bytes) {
        MutableObject<Object> rootObject = new MutableObject<>();
        ProtostuffIOUtil.mergeFrom(bytes, rootObject, rootSchema);
        return (T) rootObject.getValue();
    }

    /**
     * 反序列化对象
     */
    public static <T> T deserialize(InputStream is) throws IOException {
        return deserialize(is, useLocalBuffer());
    }

    static <T> T deserialize(InputStream is, boolean useLocalBuffer) throws IOException {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            MutableObject<Object> rootObject = new MutableObject<>();
            ProtostuffIOUtil.mergeFrom(is, rootObject, rootSchema, buffer);
            return (T) rootObject.getValue();
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
        return deserialize(bytes);
    }

}
