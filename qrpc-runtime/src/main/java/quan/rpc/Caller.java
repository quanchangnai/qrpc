package quan.rpc;

import java.lang.reflect.Array;
import java.util.Objects;

/**
 * 收到远程调用请求后用来执行实际调用的辅助类
 */
public class Caller {

    public static final Caller instance = new Caller();

    @SuppressWarnings({"unchecked", "SuspiciousSystemArraycopy"})
    protected static <T> T[] toArray(Object srcArray, Class<T> componentType) {
        if (srcArray == null) {
            return null;
        }

        Objects.requireNonNull(componentType);
        if (componentType == srcArray.getClass().getComponentType()) {
            return (T[]) srcArray;
        }

        Object[] tempArray = (Object[]) srcArray;
        T[] resultArray = (T[]) Array.newInstance(componentType, tempArray.length);
        System.arraycopy(tempArray, 0, resultArray, 0, tempArray.length);

        return resultArray;
    }

    public Object call(Service<?> service, int methodId, Object... params) throws Throwable {
        throw new IllegalArgumentException(String.format("服务[%s(%s)]不存在方法:%d", service.getClass().getName(), service.getId(), methodId));
    }

}
