package quan.rpc;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import quan.message.CodedBuffer;
import quan.message.DefaultCodedBuffer;
import quan.rpc.serialize.ObjectReader;
import quan.rpc.serialize.ObjectWriter;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程池工作者
 *
 * @author quanchangnai
 */
public class ThreadPoolWorker extends Worker {

    private final int nThreads;

    private final AtomicInteger nextCallId = new AtomicInteger(1);

    private final Lock delayedResultsLock = new ReentrantLock();

    private final Lock sortedPromisesLock = new ReentrantLock();

    protected ThreadPoolWorker(Node node, int nThreads) {
        super(node);
        this.nThreads = Math.max(1, nThreads);
    }

    @Override
    protected <K, V> Map<K, V> newMap() {
        return new ConcurrentHashMap<>();
    }

    @Override
    protected TimerQueue newTimerQueue() {
        return new TimerQueue() {

            @Override
            protected <T> Collection<T> newTempTimerTasks() {
                return new ConcurrentLinkedQueue<>();
            }

            @Override
            protected void runTimer(Timer timer) {
                execute(() -> super.runTimer(timer));
            }
        };
    }

    protected ExecutorService newExecutor() {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("worker-" + getId() + "-%d")
                .wrappedFactory(this::newThread)
                .build();

        return Executors.newFixedThreadPool(nThreads, threadFactory);
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
    protected void initService(Service service) {
        execute(() -> super.initService(service));
    }

    @Override
    protected void destroyService(Service service) {
        execute(() -> super.destroyService(service));
    }

    @Override
    protected void updateService(Service service) {
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

}
