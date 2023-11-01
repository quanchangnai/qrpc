package quan.rpc;

import java.util.Objects;

/**
 * 服务方法可以先返回延迟结果，过一段时间后再使用它设置真实返回值
 *
 * @author quanchangnai
 */
public final class DelayedResult<R> extends Promise<R> {

    private int originNodeId;

    //服务方法参数或返回结果的安全修饰符
    private int securityModifier;

    DelayedResult(Worker worker) {
        super(worker);
    }

    int getOriginNodeId() {
        return originNodeId;
    }

    void setOriginNodeId(int originNodeId) {
        this.originNodeId = originNodeId;
    }

    int getSecurityModifier() {
        return securityModifier;
    }

    void setSecurityModifier(int securityModifier) {
        this.securityModifier = securityModifier;
    }

    void setHandler() {
        then(r -> {
            this.worker.handleDelayedResult(this);
        }).except(e -> {
            this.worker.handleDelayedResult(this);
        });
    }

    @Override
    public void setResult(R result) {
        if (this.isFinished()) {
            throw new IllegalStateException("不能重复设置延迟结果");
        }

        this.worker.run(() -> super.setResult(result));
    }

    @Override
    public void setException(Exception exception) {
        Objects.requireNonNull(exception, "参数[exception]不能为空");

        if (this.isFinished()) {
            throw new IllegalStateException("不能重复设置延迟结果");
        }

        this.worker.run(() -> super.setException(exception));
    }

    String getExceptionStr() {
        return exception == null ? null : exception.toString();
    }

}
