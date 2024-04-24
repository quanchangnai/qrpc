package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static quan.rpc.ProxyConstructors.NODE_ID;
import static quan.rpc.ProxyConstructors.NODE_ID_AND_SERVICE_ID;
import static quan.rpc.ProxyConstructors.NO_ARGS;
import static quan.rpc.ProxyConstructors.SERVICE_ID;

/**
 * 支持远程方法调用的服务，被{@link Endpoint}标记的方法可以被远程调用
 *
 * @param <I> 服务ID的泛型
 * @author quanchangnai
 */
@ProxyConstructors({NO_ARGS, NODE_ID, SERVICE_ID, NODE_ID_AND_SERVICE_ID})
public abstract class Service<I> implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    volatile int state = 0;

    /**
     * 服务所属的线程工作者
     */
    private Worker worker;

    private Invoker invoker;

    private TimerMgr timerMgr;

    /**
     * 服务ID，一般为数字或者字符串，在同一个{@link Node}内必需保证唯一性
     */
    public abstract I getId();

    final void setWorker(Worker worker) {
        this.worker = worker;
    }

    public final Worker getWorker() {
        return worker;
    }

    final Invoker getInvoker() throws Exception {
        if (invoker == null) {
            Class<?> invokerClass = Class.forName(getClass().getName() + "Invoker");
            invoker = (Invoker) invokerClass.getField("instance").get(invokerClass);
        }

        return invoker;
    }

    /**
     * 执行任务
     */
    @Override
    public final void execute(Runnable task) {
        worker.execute(task);
    }

    public TimerMgr getTimerMgr() {
        return timerMgr;
    }

    /**
     * @see Worker#getTime()
     */
    public long getTime() {
        return worker.getTime();
    }

    /**
     * 创建一个延迟执行的定时器
     *
     * @see TimerMgr#newTimer(Runnable, long)
     */
    public Timer newTimer(Runnable task, long delay) {
        return timerMgr.newTimer(task, delay);
    }

    /**
     * 创建一个周期性执行的定时器
     *
     * @see TimerMgr#newTimer(Runnable, long, long)
     */
    public Timer newTimer(Runnable task, long delay, long period) {
        return timerMgr.newTimer(task, delay, period);
    }

    /**
     * 创建一个基于cron表达式的定时器
     *
     * @see TimerMgr#newTimer(Runnable, String)
     */
    public Timer newTimer(Runnable task, String cron) {
        return timerMgr.newTimer(task, cron);
    }

    public final <R> DelayedResult<R> newDelayedResult() {
        return worker.newDelayedResult();
    }

    final void initTimerMgr() {
        timerMgr = new TimerMgr(worker);
        timerMgr.newTimers(this);
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
