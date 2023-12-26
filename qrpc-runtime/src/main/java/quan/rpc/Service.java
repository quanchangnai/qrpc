package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * 支持远程方法调用的服务，被{@link Endpoint}标记的方法可以被远程调用
 *
 * @param <I> 服务ID的泛型
 * @author quanchangnai
 */
@ProxyConstructors({1, 2, 3, 4})
public abstract class Service<I> implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    volatile int state = 0;

    /**
     * 服务所属的线程工作者
     */
    private Worker worker;

    private Caller caller;

    private TimerQueue timerQueue;

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

    final Caller getCaller() throws Exception {
        if (caller == null) {
            Class<?> callerClass = Class.forName(getClass().getName() + "Caller");
            caller = (Caller) callerClass.getField("instance").get(callerClass);
        }

        return caller;
    }

    /**
     * 执行任务
     */
    @Override
    public final void execute(Runnable task) {
        worker.execute(task);
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
     * @see TimerQueue#newTimer(Runnable, long)
     */
    public Timer newTimer(Runnable task, long delay) {
        return timerQueue.newTimer(task, delay);
    }

    /**
     * 创建一个周期性执行的定时器
     *
     * @see TimerQueue#newTimer(Runnable, long, long)
     */
    public Timer newTimer(Runnable task, long delay, long period) {
        return timerQueue.newTimer(task, delay, period);
    }

    /**
     * 创建一个基于cron表达式的定时器
     *
     * @see TimerQueue#newTimer(Runnable, String)
     */
    public Timer newTimer(Runnable task, String cron) {
        return timerQueue.newTimer(task, cron);
    }


    public final <R> DelayedResult<R> newDelayedResult() {
        return worker.newDelayedResult();
    }

    final void initTimers() {
        timerQueue = new TimerQueue(worker);
        timerQueue.newTimers(this);
    }

    /**
     * 初始化
     */
    protected void init() {
    }

    final void updateTimers() {
        timerQueue.update();
    }

    /**
     * 刷帧
     */
    protected void update() {
    }

    /**
     * 销毁
     */
    protected void destroy() {
    }

}
