package quan.rpc;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import quan.message.CodedBuffer;
import quan.message.DefaultCodedBuffer;
import quan.rpc.serialize.ObjectReader;
import quan.rpc.serialize.ObjectWriter;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 线程池工作者
 *
 * @author quanchangnai
 */
public class ThreadPoolWorker extends Worker {

    private final AtomicInteger nextCallId = new AtomicInteger(1);

    private final Lock delayedResultsLock = new ReentrantLock();

    private final Lock sortedPromisesLock = new ReentrantLock();

    protected ThreadPoolWorker(Node node) {
        super(node);
    }

    @Override
    protected <K, V> Map<K, V> newMap() {
        return new ConcurrentHashMap<>();
    }

    protected ExecutorService newExecutor() {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("worker-" + getId() + "-%d")
                .wrappedFactory(this::newThread)
                .build();

        Node.Config config = getNode().getConfig();
        Supplier<ThreadPoolExecutor> threadPoolFactory = config.getThreadPoolFactory();

        if (threadPoolFactory != null) {
            ThreadPoolExecutor executor = threadPoolFactory.get();
            executor.setThreadFactory(threadFactory);
            return executor;
        } else {
            return new Executor(config.getThreadPoolWorkerCorePoolSize(), config.getThreadPoolWorkerMaxPoolSize(), config.getThreadPoolWorkerPoolSizeFactor(), threadFactory);
        }

    }

    @Override
    protected Thread newThread(Runnable task) {
        return new Thread(() -> {
            threadLocal.set(this);
            task.run();
        });
    }

    protected Object cloneObject(Object object) {
        CodedBuffer buffer = new DefaultCodedBuffer();
        ObjectWriter writer = getNode().getConfig().getWriterFactory().apply(buffer);
        ObjectReader reader = getNode().getConfig().getReaderFactory().apply(buffer);
        writer.write(object);
        return reader.read();
    }

    @Override
    protected void initService(Service<?> service) {
        execute(() -> super.initService(service));
    }

    @Override
    protected void destroyService(Service<?> service) {
        execute(() -> super.destroyService(service));
    }

    @Override
    protected void updateService(Service<?> service) {
        execute(() -> {
            if (getService(service.getId()) != null) {
                super.updateService(service);
            }
        });
    }

    @Override
    protected void addSortedPromise(Promise<Object> promise) {
        try {
            sortedPromisesLock.lock();
            super.addSortedPromise(promise);
        } finally {
            sortedPromisesLock.unlock();
        }
    }

    @Override
    protected void removeSortedPromise(Promise<Object> promise) {
        try {
            sortedPromisesLock.lock();
            super.removeSortedPromise(promise);
        } finally {
            sortedPromisesLock.unlock();
        }
    }

    @Override
    protected void expirePromises() {
        try {
            sortedPromisesLock.lock();
            super.expirePromises();
        } finally {
            sortedPromisesLock.unlock();
        }
    }


    @Override
    protected void expirePromise(Promise<Object> promise) {
        execute(() -> super.expirePromise(promise));
    }


    @Override
    protected void addDelayedResult(DelayedResult<Object> delayedResult) {
        try {
            delayedResultsLock.lock();
            super.addDelayedResult(delayedResult);
        } finally {
            delayedResultsLock.unlock();
        }
    }


    @Override
    protected boolean containsDelayedResult(DelayedResult<Object> delayedResult) {
        try {
            delayedResultsLock.lock();
            return super.containsDelayedResult(delayedResult);
        } finally {
            delayedResultsLock.unlock();
        }
    }

    @Override
    protected void expireDelayedResults() {
        try {
            delayedResultsLock.lock();
            super.expireDelayedResults();
        } finally {
            delayedResultsLock.unlock();
        }
    }

    @Override
    protected int getCallId() {
        return nextCallId.getAndUpdate(i -> ++i < 0 ? 1 : i);
    }

    private static class Executor extends ThreadPoolExecutor {

        private final AtomicInteger submittedTaskCount = new AtomicInteger();

        private final int poolSizeFactor;

        public Executor(int corePoolSize, int maxPoolSize, int poolSizeFactor, ThreadFactory threadFactory) {
            super(corePoolSize, maxPoolSize, 1, TimeUnit.SECONDS, new TaskQueue(), threadFactory);
            this.poolSizeFactor = Math.max(poolSizeFactor, 1);
            ((TaskQueue) getQueue()).executor = this;
        }

        @Override
        @SuppressWarnings("NullableProblems")
        public void execute(Runnable task) {
            submittedTaskCount.incrementAndGet();
            try {
                super.execute(task);
            } catch (RejectedExecutionException e) {
                submittedTaskCount.decrementAndGet();
                throw e;
            }
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            submittedTaskCount.decrementAndGet();
        }

        private static class TaskQueue extends LinkedBlockingQueue<Runnable> {

            Executor executor;

            @Override
            @SuppressWarnings("NullableProblems")
            public boolean offer(Runnable task) {
                int poolSize = executor.getPoolSize();
                if (executor.submittedTaskCount.get() > poolSize * executor.poolSizeFactor && poolSize < executor.getMaximumPoolSize()) {
                    return false;
                } else {
                    return super.offer(task);
                }
            }
        }

    }

}
