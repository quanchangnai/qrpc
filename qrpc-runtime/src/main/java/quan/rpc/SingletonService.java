package quan.rpc;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 单例服务标记
 *
 * @author quanchangnai
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SingletonService {

    /**
     * 服务ID
     *
     * @see Service#getId()
     */
    String id();

}
