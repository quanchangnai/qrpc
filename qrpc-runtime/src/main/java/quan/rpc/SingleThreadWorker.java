package quan.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单线程工作者
 *
 * @author quanchangnai
 */
public class SingleThreadWorker extends Worker {

    private Thread thread;

    private int nextCallId = 1;

    protected SingleThreadWorker(Node node) {
        super(node);
    }

    @Override
    public String getFlag() {
        return getNode().getConfig().getSingleThreadWorkerFlag() + getId();
    }

    @Override
    protected ExecutorService newExecutor() {
        return Executors.newFixedThreadPool(1, task -> thread = new Thread(() -> {
            threadLocal.set(this);
            task.run();
        }, "worker-" + getId()));
    }

    @Override
    protected int getCallId() {
        int callId = nextCallId++;
        if (nextCallId < 0) {
            nextCallId = 1;
        }
        return callId;
    }


    @Override
    protected void printUpdateIntervalLog(long updateIntervalTime) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement traceElement : thread.getStackTrace()) {
            sb.append("\tat ").append(traceElement).append("\n");
        }
        logger.error("线程工作者[{}]帧率过低,距离上次刷帧已经过了{}ms,线程[{}]可能执行了耗时任务\n{}", getId(), updateIntervalTime, thread, sb);
    }
}
