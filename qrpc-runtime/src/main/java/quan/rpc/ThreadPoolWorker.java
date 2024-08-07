package quan.rpc;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import quan.rpc.Node.Config.ThreadPoolParam;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池工作者
 *
 * @author quanchangnai
 */
public class ThreadPoolWorker extends Worker {

    private final ThreadPoolParam param;

    private final AtomicInteger nextCallId = new AtomicInteger(1);

    protected ThreadPoolWorker(Node node, ThreadPoolParam param) {
        super(node);
        this.param = param;
    }

    @Override
    public String getFlag() {
        return param.getFlag();
    }

    @Override
    protected <K, V> Map<K, V> newMap() {
        //保证线程安全
        return new ConcurrentHashMap<>();
    }

    @Override
    protected <E> Set<E> newSet() {
        return Collections.synchronizedSet(new HashSet<>());
    }

    @Override
    protected <E extends Comparable<E>> SortedSet<E> newSortedSet() {
        return new ConcurrentSkipListSet<>();
    }

    protected ExecutorService newExecutor() {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("worker-" + getId() + "-%d")
                .wrappedFactory(task -> new Thread(() -> {
                    threadLocal.set(this);
                    task.run();
                }))
                .build();

        return new Executor(param.getCorePoolSize(), param.getMaxPoolSize(), param.getPoolSizeFactor(), threadFactory);
    }

    @Override
    protected void initService(Service<?> service) {
        execute(service::init$);
    }

    @Override
    protected void destroyService(Service<?> service) {
        execute(service::destroy$);
    }

    @Override
    protected int getCallId() {
        return nextCallId.getAndUpdate(i -> ++i < 0 ? 1 : i);
    }

    @Override
    protected Object cloneObject(Object object) {
        return SerializeUtils.clone(object, false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + getId() +
                ",flag=" + getFlag() +
                ",corePoolSize=" + param.getCorePoolSize() +
                ",maxPoolSize=" + param.getMaxPoolSize() +
                ",poolSizeFactor=" + param.getPoolSizeFactor() +
                '}';
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
