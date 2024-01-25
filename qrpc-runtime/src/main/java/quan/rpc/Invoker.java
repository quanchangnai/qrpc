package quan.rpc;

/**
 * 调用目标方法的辅助类，收到远程调用请求后用来执行实际调用操作
 */
public class Invoker {

    public static final Invoker instance = new Invoker();

    /**
     * 调用目标方法
     */
    public Object invoke(Service<?> service, int methodId, Object... params) throws Throwable {
        throw new IllegalArgumentException(String.format("服务[%s(%s)]不存在方法:%d", service.getClass().getName(), service.getId(), methodId));
    }

    /**
     * 返回方法标签
     */
    public String getMethodLabel(int methodId) {
        return null;
    }

    /**
     * 返回方法调用的过期时间
     */
    public int getExpiredTime(int methodId) {
        return 0;
    }

}
