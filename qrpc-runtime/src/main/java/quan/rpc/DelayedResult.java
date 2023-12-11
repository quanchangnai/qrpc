package quan.rpc;

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

    @Override
    public void setResult(R r) {
        if (this.isDone()) {
            throw new IllegalStateException("不能重复设置");
        } else if (Worker.current() == worker) {
            super.setResult(r);
        } else {
            worker.execute(() -> super.setResult(r));
        }
    }

    @Override
    public void setException(Throwable e) {
        if (this.isDone()) {
            throw new IllegalStateException("不能重复设置");
        } else if (Worker.current() == worker) {
            super.setException(e);
        } else {
            worker.execute(() -> super.setException(e));
        }
    }

    String getExceptionStr() {
        Throwable e = getException();
        return e == null ? null : e.toString();
    }

}
