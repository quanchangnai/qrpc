package quan.rpc;

/**
 * 服务ID解析器，一定要保证线程安全，尽量不要加锁
 *
 * @author quanchangnai
 */
public interface ServiceIdResolver {

    /**
     * 解析目标服务ID，一般是针对单例服务做统一返回，非单例服务这里一般返回null
     *
     * @param proxy 服务代理
     * @return 目标服务的ID
     */
    Object resolveServiceId(Proxy proxy);

    /**
     * 是否缓存解析出来的服务ID
     */
    default boolean isCacheServiceId(Proxy proxy) {
        return true;
    }

}
