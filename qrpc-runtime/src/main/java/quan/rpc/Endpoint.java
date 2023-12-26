package quan.rpc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标记{@link Service}的方法可被远程调用
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface Endpoint {

    /**
     * 标记方法的所有参数都是安全的<br/>
     * 原生类型及其包装类型等不可变类型一定是安全的
     */
    boolean safeArgs() default false;

    /**
     * 标记方法的返回结果是安全的<br/>
     * 原生类型及其包装类型等不可变类型一定是安全的
     */
    boolean safeReturn() default true;

    /**
     * 调用方法的过期时间(秒)，不能超过{@link Node.Config#getMaxCallTtl()}，小于等于0代表使用默认值:{@link Node.Config#getCallTtl()}
     */
    int expiredTime() default 0;

}
