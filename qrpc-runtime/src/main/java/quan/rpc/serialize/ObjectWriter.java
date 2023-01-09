package quan.rpc.serialize;

import quan.message.CodedBuffer;
import quan.message.Message;
import quan.rpc.protocol.Protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

/**
 * 用于对象序列化
 *
 * @author quanchangnai
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ObjectWriter {

    protected TransferableRegistry transferableRegistry;

    private final CodedBuffer buffer;

    public ObjectWriter(CodedBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    public void setTransferableRegistry(TransferableRegistry transferableRegistry) {
        this.transferableRegistry = Objects.requireNonNull(transferableRegistry);
    }

    public CodedBuffer getBuffer() {
        return buffer;
    }

    public void write(Object value) {
        if (value == null) {
            buffer.writeInt(ObjectType.NULL);
            return;
        }

        Class<?> clazz = value.getClass();
        if (clazz == Byte.class) {
            buffer.writeInt(ObjectType.BYTE);
            buffer.writeByte((Byte) value);
        } else if (clazz == byte[].class) {
            buffer.writeInt(ObjectType.BYTE_ARRAY);
            buffer.writeBytes((byte[]) value);
        } else if (clazz == Boolean.class) {
            buffer.writeInt(ObjectType.BOOLEAN);
            buffer.writeBool((Boolean) value);
        } else if (clazz == boolean[].class) {
            write((boolean[]) value);
        } else if (clazz == Short.class) {
            buffer.writeInt(ObjectType.SHORT);
            buffer.writeShort((Short) value);
        } else if (clazz == short[].class) {
            write((short[]) value);
        } else if (clazz == Integer.class) {
            buffer.writeInt(ObjectType.INTEGER);
            buffer.writeInt((Integer) value);
        } else if (clazz == OptionalInt.class) {
            write((OptionalInt) value);
        } else if (clazz == int[].class) {
            write((int[]) value);
        } else if (clazz == Long.class) {
            buffer.writeInt(ObjectType.LONG);
            buffer.writeLong((Long) value);
        } else if (clazz == OptionalLong.class) {
            write((OptionalLong) value);
        } else if (clazz == long[].class) {
            write((long[]) value);
        } else if (clazz == Float.class) {
            buffer.writeInt(ObjectType.FLOAT);
            buffer.writeFloat((Float) value);
        } else if (clazz == float[].class) {
            write((float[]) value);
        } else if (clazz == Double.class) {
            buffer.writeInt(ObjectType.DOUBLE);
            buffer.writeDouble((Double) value);
        } else if (clazz == OptionalDouble.class) {
            write((OptionalDouble) value);
        } else if (clazz == double[].class) {
            write((double[]) value);
        } else if (clazz == String.class) {
            buffer.writeInt(ObjectType.STRING);
            buffer.writeString((String) value);
        } else if (clazz == String[].class) {
            write((String[]) value);
        } else if (clazz == Object.class) {
            buffer.writeInt(ObjectType.OBJECT);
        } else if (value instanceof Object[]) {
            write((Object[]) value);
        } else if (value instanceof Enum) {
            write((Enum<?>) value);
        } else if (value instanceof Collection) {
            write((Collection<?>) value);
        } else if (value instanceof Map) {
            write((Map<?, ?>) value);
        } else if (value instanceof Protocol) {
            write((Protocol) value);
        } else if (value instanceof Transferable) {
            write((Transferable) value);
        } else if (value instanceof Message) {
            write(((Message) value));
        } else if (value instanceof Serializable) {
            //对象流序列化优先级最低
            write((Serializable) value);
        } else {
            writeOther(value);
        }
    }

    private void write(boolean[] array) {
        buffer.writeInt(ObjectType.BOOLEAN_ARRAY);
        buffer.writeInt(array.length);
        for (boolean v : array) {
            buffer.writeBool(v);
        }
    }

    private void write(short[] array) {
        buffer.writeInt(ObjectType.SHORT_ARRAY);
        buffer.writeInt(array.length);
        for (short v : array) {
            buffer.writeShort(v);
        }
    }

    private void write(OptionalInt value) {
        buffer.writeInt(ObjectType.OPTIONAL_INT);
        if (value.isPresent()) {
            buffer.writeBool(true);
            buffer.writeInt(value.getAsInt());
        } else {
            buffer.writeBool(false);
        }
    }

    private void write(int[] array) {
        buffer.writeInt(ObjectType.INT_ARRAY);
        buffer.writeInt(array.length);
        for (int v : array) {
            buffer.writeInt(v);
        }
    }

    private void write(OptionalLong value) {
        buffer.writeInt(ObjectType.OPTIONAL_LONG);
        if (value.isPresent()) {
            buffer.writeBool(true);
            buffer.writeLong(value.getAsLong());
        } else {
            buffer.writeBool(false);
        }
    }

    private void write(long[] array) {
        buffer.writeInt(ObjectType.LONG_ARRAY);
        buffer.writeInt(array.length);
        for (long v : array) {
            buffer.writeLong(v);
        }
    }

    private void write(float[] array) {
        buffer.writeInt(ObjectType.FLOAT_ARRAY);
        buffer.writeInt(array.length);
        for (float v : array) {
            buffer.writeFloat(v);
        }
    }

    private void write(OptionalDouble value) {
        buffer.writeInt(ObjectType.OPTIONAL_DOUBLE);
        if (value.isPresent()) {
            buffer.writeBool(true);
            buffer.writeDouble(value.getAsDouble());
        } else {
            buffer.writeBool(false);
        }
    }

    private void write(double[] array) {
        buffer.writeInt(ObjectType.DOUBLE_ARRAY);
        buffer.writeInt(array.length);
        for (double v : array) {
            buffer.writeDouble(v);
        }
    }

    private void write(String[] array) {
        buffer.writeInt(ObjectType.STRING_ARRAY);
        buffer.writeInt(array.length);
        for (String v : array) {
            buffer.writeString(v);
        }
    }

    private void write(Object[] array) {
        buffer.writeInt(ObjectType.OBJECT_ARRAY);
        buffer.writeInt(array.length);
        for (Object v : array) {
            write(v);
        }
    }

    protected void write(Enum<?> value) {
        buffer.writeInt(ObjectType.ENUM);
        write(value.getDeclaringClass().getName());
        write(value.name());
    }

    protected void write(Collection<?> collection) {
        int type;
        if (collection instanceof SortedSet) {
            if (collection.isEmpty() || ((SortedSet<?>) collection).first() instanceof Comparable) {
                type = ObjectType.SORTED_SET;
            } else {
                //SortedSet的元素没有实现Comparable时当做HashSet处理
                type = ObjectType.HASH_SET;
            }
        } else if (collection instanceof Set) {
            type = ObjectType.HASH_SET;
        } else if (collection instanceof LinkedList) {
            type = ObjectType.LINKED_LIST;
        } else if (collection instanceof ArrayDeque) {
            type = ObjectType.ARRAY_DEQUE;
        } else {
            //其他集合类型都当做ArrayList处理
            type = ObjectType.ARRAY_LIST;
        }

        buffer.writeInt(type);
        buffer.writeInt(collection.size());
        collection.forEach(this::write);
    }

    protected void write(Map<?, ?> map) {
        int type;
        if (map instanceof SortedMap) {
            if (map.isEmpty() || ((SortedMap<?, ?>) map).firstKey() instanceof Comparable) {
                type = ObjectType.SORTED_MAP;
            } else {
                //SortedMap的key没有实现Comparable时当做HashMap处理
                type = ObjectType.HASH_MAP;
            }
        } else {
            //其他Map类型都当做HashMap处理
            type = ObjectType.HASH_MAP;
        }

        buffer.writeInt(type);
        buffer.writeInt(map.size());
        map.forEach((k, v) -> {
            write(k);
            write(v);
        });
    }

    protected void write(Protocol protocol) {
        buffer.writeInt(ObjectType.PROTOCOL);
        buffer.writeInt(Protocol.getRegistry().getId(protocol.getClass()));
        protocol.transferTo(this);
    }

    protected void write(Transferable transferable) {
        buffer.writeInt(ObjectType.TRANSFERABLE);
        buffer.writeInt(transferableRegistry.getId(transferable.getClass()));
        transferable.transferTo(this);
    }

    protected void write(Message message) {
        buffer.writeInt(ObjectType.MESSAGE);
        message.encode(buffer);
    }

    protected void write(Serializable serializable) {
        buffer.writeInt(ObjectType.SERIALIZABLE);
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(serializable);
            buffer.writeBytes(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeOther(Object other) {
        throw new RuntimeException("不支持的数据类型:" + other.getClass());
    }

}
