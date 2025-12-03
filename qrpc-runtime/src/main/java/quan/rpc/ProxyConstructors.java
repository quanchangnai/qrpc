package quan.rpc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 定义服务代理支持哪些构造方法，可选值：1-9，参考{@link #NO_ARGS}等9个常量
 *
 * @author quanchangnai
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
@Inherited
public @interface ProxyConstructors {

    int[] value();

    /**
     * 无参构造服务代理，目标节点ID和服务ID需要{@link NodeIdResolver}和{@link ServiceIdResolver}解析出来
     */
    int NO_ARGS = 1;

    /**
     * 指定[节点ID]构造服务代理，目标服务ID需要{@link ServiceIdResolver}解析出来
     */
    int NODE_ID = 2;

    /**
     * 指定[服务ID]构造服务代理，目标节点ID需要{@link NodeIdResolver}解析出来
     */
    int SERVICE_ID = 3;

    /**
     * 指定[节点ID]和[服务ID]构造服务代理
     */
    int NODE_ID_AND_SERVICE_ID = 4;

    /**
     * 指定[节点ID解析器]构造服务代理，目标节点ID和服务ID需要{@link NodeIdResolver}和{@link ServiceIdResolver}解析出来
     */
    int NODE_ID_RESOLVER = 5;

    /**
     * 指定[节点ID解析器]和[服务ID]构造服务代理，目标节点ID需要{@link NodeIdResolver}解析出来
     */
    int NODE_ID_RESOLVER_AND_SERVICE_ID = 6;

    /**
     * 指定[分片键]构造服务代理，目标节点ID和服务ID需要{@link NodeIdResolver}和{@link ServiceIdResolver}使用[分片键]计算出来
     */
    int SHARDING_KEY = 7;

}
