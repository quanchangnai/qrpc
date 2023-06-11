package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * 支持远程方法调用的服务，被{@link Endpoint}标记的方法可以被远程调用
 *
 * @author quanchangnai
 */
public abstract class Service implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 单例服务ID
     */
    private Object id;

    /**
     * 服务所属的工作线程
     */
    Worker worker;

    private Caller caller;

    /**
     * 服务ID，在同一个{@link Node}内必需保证唯一性，非单例服务应该覆盖此方法
     */
    public Object getId() {
        if (id != null) {
            return id;
        }
        SingletonService singletonService = getClass().getAnnotation(SingletonService.class);
        if (singletonService != null) {
            id = singletonService.id();
            return id;
        }
        throw new IllegalStateException("服务ID不存在");
    }

    public final Worker getWorker() {
        return worker;
    }

    final Object call(int methodId, Object... params) throws Throwable {
        if (caller == null) {
            Class<?> callerClass = Class.forName(getClass().getName() + "Caller");
            this.caller = (Caller) callerClass.getField("instance").get(callerClass);
        }
        return caller.call(this, methodId, params);
    }

    /**
     * 执行任务
     */
    @Override
    @SuppressWarnings("NullableProblems")
    public final void execute(Runnable task) {
        worker.execute(task);
    }

    /**
     * 延迟执行任务
     *
     * @see Worker#execute(Runnable, long)
     */
    public void execute(Runnable task, long delay) {
        worker.execute(task, delay);
    }

    /**
     * 周期性执行任务
     *
     * @see Worker#execute(Runnable, long, long) (Runnable, long, long)
     */
    public void execute(Runnable task, long initDelay, long period) {
        worker.execute(task, initDelay, period);
    }

    public final <R> DelayedResult<R> newDelayedResult() {
        return worker.newDelayedResult();
    }

    /**
     * 初始化
     */
    protected void init() {
    }

    /**
     * 销毁
     */
    protected void destroy() {
    }

}
