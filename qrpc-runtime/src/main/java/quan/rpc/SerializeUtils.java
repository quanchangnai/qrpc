package quan.rpc;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;

/**
 * 序列化工具
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SerializeUtils {

    private static final Schema<MutableObject> rootSchema = RuntimeSchema.getSchema(MutableObject.class);

    private static final ThreadLocal<LinkedBuffer> localBuffer = ThreadLocal.withInitial(LinkedBuffer::allocate);

    private static final GenericObjectPool<LinkedBuffer> bufferPool;

    static {
        int maxTotal = Integer.parseInt(System.getProperty("qrpc.serialize.pooled_buffer_max_total", "32"));

        GenericObjectPoolConfig<LinkedBuffer> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(Math.max(maxTotal, 8));
        poolConfig.setMinIdle(4);
        poolConfig.setSoftMinEvictableIdleDuration(Duration.ofMinutes(5));

        PooledObjectFactory<LinkedBuffer> factory = new BasePooledObjectFactory<LinkedBuffer>() {
            @Override
            public LinkedBuffer create() {
                return LinkedBuffer.allocate();
            }

            @Override
            public PooledObject<LinkedBuffer> wrap(LinkedBuffer obj) {
                return new DefaultPooledObject<>(obj);
            }
        };

        bufferPool = new GenericObjectPool<>(factory, poolConfig);
    }


    private static LinkedBuffer allocBuffer(boolean useLocalBuffer) {
        if (useLocalBuffer) {
            return localBuffer.get();
        } else {
            try {
                return bufferPool.borrowObject();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void releaseBuffer(LinkedBuffer buffer, boolean useLocalBuffer) {
        buffer.clear();
        if (!useLocalBuffer) {
            bufferPool.returnObject(buffer);
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

    /**
     * 序列化对象
     */
    public static <T> byte[] serialize(T obj, boolean useLocalBuffer) {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        byte[] bytes;

        try {
            bytes = ProtostuffIOUtil.toByteArray(new MutableObject<>(obj), rootSchema, buffer);
        } finally {
            releaseBuffer(buffer, useLocalBuffer);
        }

        return bytes;
    }

    /**
     * 序列化对象
     */
    public static <T> void serialize(T obj, OutputStream os) throws IOException {
        serialize(obj, os, useLocalBuffer());
    }

    /**
     * 序列化对象
     */
    public static <T> void serialize(T obj, OutputStream os, boolean useLocalBuffer) throws IOException {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            ProtostuffIOUtil.writeTo(os, new MutableObject<>(obj), rootSchema, buffer);
        } finally {
            releaseBuffer(buffer, useLocalBuffer);
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

    /**
     * 反序列化对象
     */
    public static <T> T deserialize(InputStream is, boolean useLocalBuffer) throws IOException {
        LinkedBuffer buffer = allocBuffer(useLocalBuffer);

        try {
            MutableObject<Object> rootObject = new MutableObject<>();
            ProtostuffIOUtil.mergeFrom(is, rootObject, rootSchema, buffer);
            return (T) rootObject.getValue();
        } finally {
            releaseBuffer(buffer, useLocalBuffer);
        }
    }

    /**
     * 复制对象
     */
    public static <T> T clone(T obj) {
        byte[] bytes = serialize(obj);
        return deserialize(bytes);
    }

    /**
     * 复制对象
     */
    public static <T> T clone(T obj, boolean useLocalBuffer) {
        byte[] bytes = serialize(obj, useLocalBuffer);
        return deserialize(bytes);
    }


}
