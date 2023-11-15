package quan.rpc;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * @author quanchangnai
 */
public class TimerQueue {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PriorityQueue<TimerTask> timerTaskQueue = new PriorityQueue<>();

    private final List<TimerTask> tempTimerTasks = new ArrayList<>();

    private final CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    private final Worker worker;

    private final boolean concurrent;

    public TimerQueue(Worker worker) {
        this.worker = Objects.requireNonNull(worker);
        this.concurrent = worker instanceof ThreadPoolWorker;
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

        return addTimerTask(task, delay, 0, null);
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

        int updateInterval = worker.getNode().getConfig().getUpdateInterval();
        if (period < updateInterval) {
            throw new IllegalArgumentException("参数[period]不能小于" + updateInterval);
        }

        return addTimerTask(task, delay, period, null);
    }

    /**
     * 创建一个基于cron表达式的定时器
     *
     * @param task 定时器任务
     * @param cron cron表达式
     */
    public Timer newTimer(Runnable task, String cron) {
        return addTimerTask(task, 0, 0, cron);
    }

    private Timer addTimerTask(Runnable task, long delay, long period, String cron) {
        Objects.requireNonNull(task, "参数[task]不能为空");

        TimerTask timerTask = new TimerTask();

        timerTask.task = task;
        timerTask.delay = delay;
        timerTask.period = period;

        if (cron != null) {
            timerTask.cronTime = ExecutionTime.forCron(cronParser.parse(cron));
        }

        timerTask.calcTime();

        addTimerTask(timerTask);

        return timerTask;
    }

    private void addTimerTask(TimerTask timerTask) {
        if (concurrent) {
            synchronized (tempTimerTasks) {
                tempTimerTasks.add(timerTask);
            }
        } else {
            tempTimerTasks.add(timerTask);
        }

    }

    private void moveTimerTasks() {
        if (concurrent) {
            synchronized (tempTimerTasks) {
                timerTaskQueue.addAll(tempTimerTasks);
                tempTimerTasks.clear();
            }
        } else {
            timerTaskQueue.addAll(tempTimerTasks);
            tempTimerTasks.clear();
        }
    }

    private void runTimerTask(TimerTask timerTask) {
        if (concurrent) {
            worker.execute(timerTask::run);
        } else {
            timerTask.run();
        }
    }

    public void update() {
        if (concurrent) {
            synchronized (timerTaskQueue) {
                doUpdate();
            }
        } else {
            doUpdate();
        }
    }

    private void doUpdate() {
        try {
            moveTimerTasks();

            if (timerTaskQueue.isEmpty()) {
                return;
            }

            TimerTask timerTask = timerTaskQueue.peek();
            while (timerTask != null && (timerTask.isTimeUp() || timerTask.isCancelled())) {
                timerTaskQueue.poll();

                if (!timerTask.isCancelled()) {
                    timerTask.calcTime();
                    runTimerTask(timerTask);
                    if (!timerTask.isDone() && !timerTask.isCancelled()) {
                        addTimerTask(timerTask);
                    }
                }

                timerTask = timerTaskQueue.peek();
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }


    /**
     * 定时任务
     */
    private class TimerTask implements Timer, Comparable<TimerTask> {

        /**
         * 期望执行时间
         */
        long time;

        long delay;

        /**
         * 执行周期，小于1代表该任务不是周期任务
         */
        long period;

        /**
         * Cron任务执行时间，null代表该任务不是Cron任务
         */
        ExecutionTime cronTime;

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
            return time > 0 && time < worker.getTime();
        }

        @Override
        public int compareTo(TimerTask other) {
            return Long.compare(this.time, other.time);
        }

        void calcTime() {
            if (time == 0 && delay >= 0) {
                time = worker.getTime() + delay;
            } else if (period > 0) {
                time = worker.getTime() + period;
            } else if (cronTime != null) {
                Instant instant = Instant.ofEpochMilli(worker.getTime());
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
                Optional<ZonedDateTime> nextTime = cronTime.nextExecution(zonedDateTime);
                time = nextTime.map(dateTime -> dateTime.toInstant().toEpochMilli()).orElse(-1L);
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
