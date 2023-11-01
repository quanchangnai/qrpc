package quan.rpc;

/**
 * 支持刷帧的服务{@link Service}需要实现此接口
 *
 * @author quanchangnai
 */
public interface Updatable {

    /**
     * 刷帧
     */
    void update();

}
