package quan.rpc;

/**
 * 定时器
 *
 * @author quanchangnai
 */
public interface Timer {

    void cancel();

    boolean isCancelled();

    boolean isDone();

}
