package quan.rpc;

import org.apache.commons.lang3.StringUtils;

/**
 * 远程调用异常
 *
 * @author quanchangnai
 */
public class CallException extends RuntimeException {

    private long callId;

    private String signature;

    private boolean timeout;

    public CallException() {
    }

    public CallException(String message) {
        super(message);
    }

    public CallException(String message, boolean timeout) {
        super(message);
        this.timeout = timeout;
    }

    public CallException(String message, Throwable cause) {
        super(message, cause);
    }

    protected void setCallId(long callId) {
        this.callId = callId;
    }

    protected void setSignature(String signature) {
        this.signature = signature;
    }

    public boolean isTimeout() {
        return timeout;
    }

    @Override
    public String getMessage() {
        String message = "调用";

        if (callId > 0 && signature != null) {
            message = String.format("[%s]方法[%s]", callId, signature);
        }

        if (timeout) {
            message += "超时";
        } else {
            message += "返回异常";
        }

        if (!StringUtils.isBlank(super.getMessage())) {
            message += ":" + super.getMessage();
        }

        return message;
    }

    public static CallException create(String exceptionStr) {
        if (exceptionStr != null) {
            return new CallException(exceptionStr);
        } else {
            return null;
        }
    }

}
