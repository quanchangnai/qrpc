package quan.rpc;

import java.lang.annotation.*;

/**
 * 定时器
 *
 * @author quanchangnai
 */
public interface Timer {

    void cancel();

    boolean isCancelled();

    boolean isDone();


    /**
     * 标记无参方法为周期性定时任务
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @interface Period {
        /**
         * 周期时间(毫秒)
         */
        long value();
    }

    /**
     * 标记无参方法为基于Cron表达式的定时任务
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @interface Cron {
        String value();
    }

}
