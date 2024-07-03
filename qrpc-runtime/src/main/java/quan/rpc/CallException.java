package quan.rpc;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * 远程调用异常
 *
 * @author quanchangnai
 */
public class CallException extends RuntimeException {

    private long callId;

    /**
     * 目标节点
     */
    private int nodeId;

    /**
     * 目标方法信息
     */
    private String methodInfo;

    private Reason reason;

    private String message;

    public CallException(Reason reason) {
        this.reason = Objects.requireNonNull(reason);
    }

    public CallException(String message, Reason reason) {
        super(message);
        this.reason = Objects.requireNonNull(reason);
    }

    public CallException(Throwable cause, Reason reason) {
        super(cause);
        this.reason = Objects.requireNonNull(reason);
    }

    public CallException(String message, Throwable cause, Reason reason) {
        super(message == null ? cause.toString() : message, cause);
        this.reason = Objects.requireNonNull(reason);
    }

    protected void setCallId(long callId) {
        this.callId = callId;
    }

    public long getCallId() {
        return callId;
    }

    public int getNodeId() {
        return nodeId;
    }

    protected void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    protected void setMethodInfo(String methodInfo) {
        this.methodInfo = methodInfo;
    }

    public String getMethodInfo() {
        return methodInfo;
    }

    public Reason getReason() {
        return reason;
    }

    @Override
    public String getMessage() {
        if (message != null) {
            return message;
        }

        message = "调用方法失败";

        if (callId > 0) {
            message += ",callId:" + callId;
        }

        if (nodeId > 0) {
            message += ",目标节点:" + nodeId;
        }

        if (methodInfo != null) {
            message += ",目标方法:" + methodInfo;
        }

        message += ",原因:" + reason;

        if (!StringUtils.isBlank(super.getMessage())) {
            message += ",详情:" + super.getMessage();
        }

        return message;
    }

    public enum Reason {

        /**
         * 目标节点无效
         */
        INVALID_NODE,

        /**
         * 远程节点的连接断开了
         */
        DISCONNECTED,

        /**
         * 发送协议出错
         */
        SEND_ERROR,

        /**
         * 远程错误，一般是目标方法执行出错
         */
        REMOTE_ERROR,

        /**
         * 等待超时
         */
        TIME_OUT,

    }

}
