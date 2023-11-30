package quan.rpc;

import java.lang.annotation.*;

/**
 * 定义服务代理支持哪些构造方法，可选值：1-6，参考{@link Proxy}的6种构造方法
 *
 * @author quanchangnai
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
@Inherited
public @interface ProxyConstructors {
    int[] value();
}
