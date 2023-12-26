package quan.rpc;

/**
 * 收到远程调用请求后用来执行实际调用的辅助类
 */
public class Caller {

    public static final Caller instance = new Caller();

    public Object call(Service<?> service, int methodId, Object... params) throws Throwable {
        throw new IllegalArgumentException(String.format("服务[%s(%s)]不存在方法:%d", service.getClass().getName(), service.getId(), methodId));
    }

    public String getMethodLabel(int methodId) {
        return null;
    }

    public int getExpiredTime(int methodId) {
        return 0;
    }

}
