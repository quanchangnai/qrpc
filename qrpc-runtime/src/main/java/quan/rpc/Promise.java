package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 处理异步调用的结果或异常的辅助类，类似于try-catch-finally代码块<br>
 * then方法用于处理结果，exceptionally方法用于处理异常，没有处理的异常会往后传递，completely方法则是最终处理，
 * 这些方法都会返回一个新的Promise对象用于后续处理流程，具体参考如下代码片段
 *
 * <pre> {@code
 * try {
 *      r = s1();
 *      s2(r);
 *      //p1 = a1()
 *      //p2 = p1.then(r -> a2(r))
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
     * 被调用方法的签名字符串
     */
    private String signature;

    protected final Worker worker;

    /**
     * 过期时间
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

    protected String getSignature() {
        return signature;
    }

    protected void setSignature(String signature) {
        this.signature = signature;
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
            CallException callException = (CallException) e;
            callException.setCallId(callId);
            callException.setSignature(signature);
        }

        future.completeExceptionally(e);
    }

    protected Throwable getException() {
        if (isFailed()) {
            try {
                future.get();
            } catch (Throwable e) {
                return realException(e);
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
            setException(new CallException(null, true));
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
    public Promise<Void> then(Runnable handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenRun(handler));
    }

    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 结果处理器
     * @return 一个新的Promise，用于处理异常情况
     */
    public Promise<Void> then(Consumer<? super R> handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenAccept(handler));
    }

    /**
     * 设置异步调用成功返回时的处理器
     *
     * @param handler 结果处理器，处理结果并返回另一个带结果的Promise
     * @return 一个新的Promise，其结果和handler返回的Promise一样
     */

    public <U> Promise<U> then(Function<? super R, ? extends Promise<U>> handler) {
        checkHandler(handler);
        return new Promise<>(worker, future.thenCompose(r -> {
            Promise<U> p = handler.apply(r);
            return p == null ? null : p.future;
        }));
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
        logger.error("", realException(e));
    }

    private static Throwable realException(Throwable e) {
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
                    handler.accept(realException(e1));
                }
                p.future.complete(null);
            } catch (Throwable e2) {
                p.future.completeExceptionally(e2);
            }

        });

        return p;
    }


    /**
     * 用来设置异步调用返回后的最终处理器,不管是成功返回还是异常返回，类似finally代码块
     *
     * @param handler 该处理器不应该再抛出异常，但是如果抛出了异常，该异常将优先往后传递
     * @return 一个新的Promise，用于处理参数handler的执行情况
     */
    public Promise<Void> completely(Runnable handler) {
        checkHandler(handler);
        return completely0(handler);
    }

    protected Promise<Void> completely0(Runnable handler) {
        Promise<Void> p = new Promise<>(worker);

        future.whenComplete((r, e1) -> {
            try {
                handler.run();
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

    @Override
    public int compareTo(Promise<?> other) {
        return Long.compare(this.expiredTime, other.expiredTime);
    }

}
