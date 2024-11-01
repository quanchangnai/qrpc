package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 处理异步调用的结果或异常的辅助类，类似于try-catch-finally代码块<br>
 * thenXxx方法用于处理结果，exceptionally方法用于处理异常，没有处理的异常会往后传递，completely方法则是最终处理，
 * 这些方法都会返回一个新的Promise对象用于后续处理流程，具体参考如下代码片段
 *
 * <pre> {@code
 * try {
 *      r = s1();
 *      s2(r);
 *      //p1 = a1()
 *      //p2 = p1.thenAccept(r -> a2(r))
 * } catch (Exception e) {
 *      s3(e);
 *      //p3 = p2.exceptionally(e -> a3(e))
 * } finally {
 *      s4();
 *      //p4 = p3.completely(() -> a4())
 * }
 * }</pre>
 *
 * @author quanchangnai
 */
public class Promise<R> implements Comparable<Promise<?>> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 调用ID
     */
    private long callId;

    /**
     * 目标节点
     */
    private int nodeId;

    /**
     * 目标方法信息
     */
    private String methodInfo;

    /**
     * 所属工作者
     */
    protected final Worker worker;

    /**
     * 过期时间戳
     */
    private long expiredTime;

    protected final CompletableFuture<R> future;

    private Object handler;


    protected Promise(Worker worker) {
        this(worker, new CompletableFuture<>());
    }

    protected Promise(Worker worker, CompletableFuture<R> future) {
        this.worker = Objects.requireNonNull(worker);
        this.future = future;
        this.calcExpiredTime(0);
        this.setDefaultExceptionHandler();
    }


    protected long getCallId() {
        return callId;
    }

    protected void setCallId(long callId) {
        this.callId = callId;
    }

    protected int getNodeId() {
        return nodeId;
    }

    protected void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    protected String getMethodInfo() {
        return methodInfo;
    }

    protected void setMethodInfo(String methodInfo) {
        this.methodInfo = methodInfo;
    }

    public Worker getWorker() {
        return worker;
    }

    protected long getExpiredTime() {
        return expiredTime;
    }

    protected void setExpiredTime(long expiredTime) {
        this.expiredTime = expiredTime;
    }

    /**
     * 计算过期时间
     *
     * @param expiredTime 距离过期时间点的秒数
     */
    protected void calcExpiredTime(int expiredTime) {
        if (expiredTime <= 0) {
            expiredTime = worker.getNode().getConfig().getCallTtl();
        } else {
            expiredTime = Math.min(expiredTime * 1000, worker.getNode().getConfig().getMaxCallTtl());
        }

        this.expiredTime = System.currentTimeMillis() + expiredTime;
    }

    protected void setResult(R r) {
        future.complete(r);
    }

    public R getResult() {
        if (isOK()) {
            return future.getNow(null);
        } else {
            return null;
        }
    }

    protected void setException(Throwable e) {
        Objects.requireNonNull(e);

        if (e instanceof CallException) {
            setCallInfo((CallException) e);
        }

        future.completeExceptionally(e);
    }

    protected void setCallInfo(CallException callException) {
        callException.setCallId(callId);
        callException.setNodeId(nodeId);
        callException.setMethodInfo(methodInfo);
    }

    protected Throwable getException() {
        if (isFailed()) {
            try {
                future.get();
            } catch (Throwable e) {
                return getActualException(e);
            }
        }

        return null;
    }

    protected String getExceptionStr() {
        Throwable e = getException();
        return e == null ? null : e.toString();
    }

    protected void expire() {
        if (!isDone()) {
            setException(new CallException(CallException.Reason.TIME_OUT));
        }
    }

    protected boolean isExpired() {
        return System.currentTimeMillis() > expiredTime;
    }

    public boolean isDone() {
        return future.isDone();
    }

    public boolean isOK() {
        return future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally();
    }

    public boolean isFailed() {
        return future.isCancelled() || future.isCompletedExceptionally();
    }

    public static Promise<Void> allOf(Promise<?>... promises) {
        return new Promise<>(Worker.current(), CompletableFuture.allOf(toFutures(promises)));
    }

    public static Promise<Object> anyOf(Promise<?>... promises) {
        return new Promise<>(Worker.current(), CompletableFuture.anyOf(toFutures(promises)));
    }

    private static CompletableFuture<?>[] toFutures(Promise<?>... promises) {
        CompletableFuture<?>[] futures = new CompletableFuture[promises.length];
        for (int i = 0; i < promises.length; i++) {
            futures[i] = promises[i].future;
        }
        return futures;
    }

    private void checkHandler(Object handler) {
        if (this.handler == null) {
            this.handler = Objects.requireNonNull(handler, "处理器不能为空");
        } else {
            throw new IllegalStateException("不能重复设置处理器");
        }
    }

    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 不关心结果的处理器
     * @return 一个新的Promise，用于处理异常情况
     */
    public Promise<Void> thenRun(Runnable handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenRun(handler));
    }

    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 结果处理器
     * @return 一个新的Promise，用于处理异常情况
     */
    public Promise<Void> thenAccept(Consumer<? super R> handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenAccept(handler));
    }

    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 结果处理器，处理结果并返回一个新的结果
     * @return 一个新的Promise，其结果就是参数handler返回的新结果
     */
    public <U> Promise<U> thenApply(Function<? super R, ? extends U> handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenApply(handler));
    }


    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 结果处理器，处理结果并返回另一个带结果的Promise
     * @return 一个新的Promise，其结果和参数handler返回的Promise一样
     */
    public <U> Promise<U> thenCompose(Function<? super R, ? extends Promise<U>> handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenCompose(r -> {
            Promise<U> p = handler.apply(r);
            return p == null ? null : p.future;
        }));
    }


    /**
     * 设置异步调用异常返回时的处理器，类似catch代码块
     *
     * @param handler 异常处理器
     * @return 一个新的Promise，用于处理参数handler的执行情况
     */
    public Promise<Void> exceptionally(Consumer<? super Throwable> handler) {
        checkHandler(handler);
        Promise<Void> p = new Promise<>(worker);

        future.whenComplete((r, e1) -> {
            try {
                if (e1 != null) {
                    handler.accept(getActualException(e1));
                }
                p.future.complete(null);
            } catch (Throwable e2) {
                p.future.completeExceptionally(e2);
            }

        });

        return p;
    }


    /**
     * 用来设置异步调用返回后的最终处理器，不管是成功返回还是异常返回，类似finally代码块
     *
     * @param handler 该处理器不应该再抛出异常，但是如果抛出了异常，该异常将优先往后传递
     * @return 一个新的Promise，用于处理参数handler的执行情况
     */
    public Promise<Void> completely(BiConsumer<? super R, ? super Throwable> handler) {
        checkHandler(handler);
        return completely0(handler);
    }

    /**
     * @see #completely(BiConsumer)
     */
    public Promise<Void> completely(Runnable handler) {
        Objects.requireNonNull(handler, "处理器不能为空");
        return completely((r, e) -> handler.run());
    }

    protected Promise<Void> completely0(BiConsumer<? super R, ? super Throwable> handler) {
        this.handler = Objects.requireNonNull(handler, "处理器不能为空");
        Promise<Void> p = new Promise<>(worker);

        future.whenComplete((r, e1) -> {
            try {
                handler.accept(r, e1);
            } catch (Throwable e2) {
                if (e1 != null) {
                    printException(e1);
                }
                p.future.completeExceptionally(e2);
                return;
            }

            if (e1 == null) {
                p.future.complete(null);
            } else {
                p.future.completeExceptionally(e1);
            }
        });

        return p;
    }

    private void setDefaultExceptionHandler() {
        future.whenComplete((r, e) -> {
            if (handler == null && e != null) {
                //没有处理或传递的异常，默认打印一下堆栈
                printException(e);
            }
        });
    }

    private void printException(Throwable e) {
        logger.error("未处理异常", getActualException(e));
    }

    private static Throwable getActualException(Throwable e) {
        while (e instanceof CompletionException || e instanceof ExecutionException) {
            Throwable cause = e.getCause();
            if (cause != null) {
                e = cause;
            } else {
                break;
            }
        }

        return e;
    }

    @Override
    public int compareTo(Promise<?> other) {
        int r = Long.compare(this.expiredTime, other.expiredTime);

        if (r == 0) {
            r = Long.compare(this.callId, other.callId);
        }

        return r;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "callId=" + callId +
                ", methodInfo='" + methodInfo + '\'' +
                '}';
    }

}
