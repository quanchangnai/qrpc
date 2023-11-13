package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * @author quanchangnai
 */
public class TimerQueue {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PriorityQueue<TimerTask> timerTaskQueue = new PriorityQueue<>();

    private final Collection<TimerTask> tempTimerTasks = newTempTimerTasks();

    protected <T> Collection<T> newTempTimerTasks() {
        return new ArrayList<>();
    }

    /**
     * 创建一个延迟执行的定时器
     *
     * @param task  定时器任务
     * @param delay 延迟时间
     */
    public Timer newTimer(Runnable task, long delay) {
        if (delay < 0) {
            throw new IllegalArgumentException("参数[delay]不能小于0");
        }

        return addTimerTask(task, delay, 0);
    }

    /**
     * 创建一个周期性执行的定时器
     *
     * @param task   定时器任务
     * @param delay  延迟时间
     * @param period 周期时间
     */
    public Timer newTimer(Runnable task, long delay, long period) {
        if (delay < 0) {
            throw new IllegalArgumentException("参数[delay]不能小于0");
        }

        int updateInterval = Worker.current().getNode().getConfig().getUpdateInterval();
        if (period < updateInterval) {
            throw new IllegalArgumentException("参数[period]不能小于" + updateInterval);
        }

        return addTimerTask(task, delay, period);
    }

    private TimerTask addTimerTask(Runnable task, long delay, long period) {
        Objects.requireNonNull(task, "参数[task]不能为空");

        TimerTask timerTask = new TimerTask();
        timerTask.time = Worker.current().getTime() + delay;
        timerTask.period = period;
        timerTask.task = task;

        tempTimerTasks.add(timerTask);

        return timerTask;
    }

    public void update() {
        try {
            timerTaskQueue.addAll(tempTimerTasks);
            tempTimerTasks.clear();

            if (timerTaskQueue.isEmpty()) {
                return;
            }

            TimerTask timerTask = timerTaskQueue.peek();
            while (timerTask != null && (timerTask.isTimeUp() || timerTask.isCancelled())) {
                timerTaskQueue.poll();

                if (!timerTask.isCancelled()) {
                    timerTask.resetTime();
                    runTimer(timerTask);
                    if (timerTask.period > 0) {
                        tempTimerTasks.add(timerTask);
                    }
                }

                timerTask = timerTaskQueue.peek();
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    protected void runTimer(Timer timer) {
        ((TimerTask) timer).run();
    }


    /**
     * 定时任务
     */
    private class TimerTask implements Timer, Comparable<TimerTask> {

        /**
         * 期望执行时间
         */
        long time;

        /**
         * 执行周期，小于1代表该任务不是周期任务
         */
        long period;

        Runnable task;

        @Override
        public void cancel() {
            time = -2;
        }

        @Override
        public boolean isCancelled() {
            return time == -2;
        }

        @Override
        public boolean isDone() {
            return time == -1;
        }

        boolean isTimeUp() {
            return time > 0 && time < Worker.current().getTime();
        }

        @Override
        public int compareTo(TimerTask other) {
            return Long.compare(this.time, other.time);
        }

        void resetTime() {
            if (period > 0) {
                time = Worker.current().getTime() + period;
            } else {
                time = -1;
            }
        }

        void run() {
            try {
                task.run();
            } catch (Exception e) {
                logger.error("", e);
            }
        }

    }
}
