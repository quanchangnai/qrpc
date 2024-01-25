package quan.rpc;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 线程池工作者
 *
 * @author quanchangnai
 */
public class ThreadPoolWorker extends Worker {

    private final AtomicInteger nextCallId = new AtomicInteger(1);

    protected ThreadPoolWorker(Node node) {
        super(node);
    }

    @Override
    protected <K, V> Map<K, V> newMap() {
        //保证线程安全
        return new ConcurrentHashMap<>();
    }

    @Override
    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    protected <E> SortedSet<E> newSortedSet() {
        return new ConcurrentSkipListSet<>();
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
            return new Executor(config.getCoreThreadPoolSize(), config.getMaxThreadPoolSize(), config.getThreadPoolSizeFactor(), threadFactory);
        }
    }

    @Override
    protected Thread newThread(Runnable task) {
        return new Thread(() -> {
            threadLocal.set(this);
            task.run();
        });
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
    protected int getCallId() {
        return nextCallId.getAndUpdate(i -> ++i < 0 ? 1 : i);
    }

    @Override
    protected Object cloneObject(Object object) {
        return SerializeUtils.clone(object, false);
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
