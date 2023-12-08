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
 * 用于监听远程方法的调用结果等
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

    private final CompletableFuture<R> future;

    protected Promise(Worker worker) {
        this(worker, 0, null);
    }

    protected Promise(Worker worker, CompletableFuture<R> future) {
        this.worker = Objects.requireNonNull(worker);
        this.future = future;
        this.calcExpiredTime(0);
    }

    protected Promise(Worker worker, long callId, String signature) {
        this.worker = Objects.requireNonNull(worker);
        this.callId = callId;
        this.signature = signature;
        this.future = new CompletableFuture<>();
        this.calcExpiredTime(0);
    }

    protected long getCallId() {
        return callId;
    }

    protected void setCallId(long callId) {
        this.callId = callId;
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
        return future.getNow(null);
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
        if (future.isCompletedExceptionally()) {
            try {
                future.get();
            } catch (Throwable e) {
                return realException(e);
            }
        }

        return null;
    }

    protected void expire() {
        if (!isDone()) {
            CallException e = new CallException(null, true);
            setException(e);
            logger.error(e.getMessage());
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
        return future.isCompletedExceptionally();
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

    /**
     * 设置异步调用正常返回时的处理器
     *
     * @param handler 不关心结果的处理器
     * @return 一个新的无结果的Promise，用于监听异常情况
     */
    public Promise<Void> then(Runnable handler) {
        return new Promise<>(worker, future.thenRunAsync(handler, worker));
    }

    /**
     * 设置异步调用正常返回时的处理器
     *
     * @param handler 结果处理器
     * @return 一个新的无结果的Promise，用于监听异常情况
     */
    public Promise<Void> then(Consumer<? super R> handler) {
        return new Promise<>(worker, future.thenAcceptAsync(handler, worker));
    }

    /**
     * 设置异步调用正常返回时的处理器
     *
     * @param handler 结果处理器，处理结果并返回另一个带结果的Promise
     * @return 一个新的Promise，其结果和handler返回的Promise一样
     */
    public <U> Promise<U> then(Function<? super R, ? extends Promise<U>> handler) {
        return new Promise<>(worker, future.thenComposeAsync(r -> {
            Promise<U> p = handler.apply(r);
            return p == null ? null : p.future;
        }, worker));
    }

    /**
     * 设置异步调用正常或异常返回时的处理器
     *
     * @param handler 处理器
     * @return 一个新的无结果的Promise，用于监听handler的执行情况
     */
    public Promise<Void> then(BiConsumer<? super R, ? super Throwable> handler) {
        Promise<Void> p = new Promise<>(worker);

        future.whenCompleteAsync((r, e1) -> {
            try {
                handler.accept(r, realException(e1));
                p.future.complete(null);
            } catch (Throwable e2) {
                p.future.completeExceptionally(e2);
            }
        }, worker);

        return p;
    }

    /**
     * 设置异步调用异常返回时的处理器
     *
     * @param handler 异常处理器
     * @return 一个新的无结果的Promise，用于监听handler的执行情况
     */
    public Promise<Void> except(Consumer<? super Throwable> handler) {
        Promise<Void> p = new Promise<>(worker);

        future.whenCompleteAsync((r, e1) -> {
            if (e1 == null) {
                return;
            }
            try {
                handler.accept(realException(e1));
                p.future.complete(null);
            } catch (Throwable e2) {
                p.future.completeExceptionally(e2);
            }
        }, worker);

        return p;
    }

    /**
     * 设置异步调用异常返回时的处理器
     *
     * @param handler 异常处理器，处理异常并返回另一个带结果的Promise
     * @return 一个新的Promise，其结果和handler返回的Promise一样
     */
    public <U> Promise<U> except(Function<? super Throwable, ? extends Promise<U>> handler) {
        Promise<U> p1 = new Promise<>(worker);

        future.whenCompleteAsync((r, e1) -> {
            if (e1 == null) {
                return;
            }

            Promise<U> p2;

            try {
                p2 = handler.apply(realException(e1));
            } catch (Throwable e2) {
                p1.future.completeExceptionally(e2);
                return;
            }

            if (p2 == null) {
                p1.future.complete(null);
            } else
                p2.future.whenComplete((u, e3) -> {
                    if (e3 != null) {
                        p1.future.completeExceptionally(e3);
                    } else {
                        p1.future.complete(u);
                    }
                });
        }, worker);

        return p1;
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

    @Override
    public int compareTo(Promise<?> other) {
        return Long.compare(this.expiredTime, other.expiredTime);
    }

}
