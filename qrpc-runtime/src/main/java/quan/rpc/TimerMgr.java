package quan.rpc;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 定时器管理器
 *
 * @author quanchangnai
 */
public class TimerMgr {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<TimerTask> tempTimerTasks = new ArrayList<>();

    private final PriorityQueue<TimerTask> timerTaskQueue = new PriorityQueue<>();

    private final CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    private final Worker worker;

    private final boolean concurrent;

    private static final Map<Class<?>, List<Triple<MethodHandle, Long, String>>> classTimerMethods = new ConcurrentHashMap<>();

    public TimerMgr() {
        this(Worker.current());
    }

    public TimerMgr(Worker worker) {
        this.worker = Objects.requireNonNull(worker);
        this.concurrent = !worker.isSingleThread();
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
        Objects.requireNonNull(cron, "cron表达式不能为空");
        return addTimerTask(task, 0, 0, cron);
    }

    /**
     * 为指定对象上所有被{@link Timer.Period}和{@link Timer.Cron}标记过的无参方法创建定时器
     */
    public List<Timer> newTimers(Object obj) {
        Class<?> clazz = obj.getClass();
        List<Triple<MethodHandle, Long, String>> timerMethods = classTimerMethods.get(clazz);

        if (timerMethods == null) {
            timerMethods = new ArrayList<>();

            for (Method method : clazz.getDeclaredMethods()) {
                Timer.Period period = method.getAnnotation(Timer.Period.class);
                Timer.Cron cron = method.getAnnotation(Timer.Cron.class);
                if (period == null && cron == null) {
                    continue;
                }

                if (method.getParameterCount() > 0) {
                    throw new RuntimeException("定时方法不能带有参数:" + method);
                }

                try {
                    method.setAccessible(true);
                    MethodHandle methodHandle = MethodHandles.publicLookup().unreflect(method);
                    timerMethods.add(Triple.of(methodHandle, period == null ? null : period.value(), cron == null ? null : cron.value()));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            classTimerMethods.put(clazz, timerMethods);
        }

        List<Timer> timers = new ArrayList<>();

        for (Triple<MethodHandle, Long, String> timerMethod : timerMethods) {
            MethodHandle methodHandle = timerMethod.getLeft();
            Long period = timerMethod.getMiddle();
            String cron = timerMethod.getRight();

            Runnable task = () -> {
                try {
                    if (methodHandle.type().parameterCount() > 0) {
                        methodHandle.invoke(obj);
                    } else {
                        methodHandle.invoke();
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };

            if (period != null) {
                timers.add(newTimer(task, period, period));
            }

            if (cron != null) {
                timers.add(newTimer(task, cron));
            }
        }

        return timers;
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
            logger.error("定时任务执行出错", e);
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
                logger.error("定时任务执行出错", e);
            }
        }

    }
}
