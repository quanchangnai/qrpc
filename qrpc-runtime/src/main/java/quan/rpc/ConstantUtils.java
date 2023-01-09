package quan.rpc;

import javax.lang.model.type.TypeMirror;
import java.lang.reflect.Array;
import java.util.*;

import static org.apache.commons.lang3.ClassUtils.isPrimitiveOrWrapper;

/**
 * 常量工具类
 */
public class ConstantUtils {

    private static final Set<Class<?>> constantClasses = new HashSet<>();

    static {
        constantClasses.add(Object.class);
        constantClasses.add(String.class);
        constantClasses.add(OptionalInt.class);
        constantClasses.add(OptionalLong.class);
        constantClasses.add(OptionalDouble.class);
    }

    public static void addConstantClass(Class<?> clazz) {
        constantClasses.add(clazz);
    }

    /**
     * 判断对象是不是常量，包含原生类型及其包装类型等不可变类型
     */
    public static boolean isConstant(Object value) {
        if (value == null) {
            return true;
        }

        Class<?> clazz = value.getClass();
        if (clazz.isArray() && Array.getLength(value) == 0) {
            return true;
        } else {
            return isConstantClass(clazz);
        }
    }

    /**
     * 判断类是不是常量类型
     */
    public static boolean isConstantClass(Class<?> clazz) {
        return isPrimitiveOrWrapper(clazz)
                || clazz.isEnum()
                || constantClasses.contains(clazz);
    }

    public static boolean isConstantType(TypeMirror type) {
        if (type.getKind().isPrimitive()) {
            return true;
        }
        try {
            Class<?> clazz = Class.forName(type.toString());
            return isConstantClass(clazz);
        } catch (Exception e) {
            return false;
        }
    }

}
