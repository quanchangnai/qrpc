package quan.rpc;

import org.apache.commons.lang3.StringUtils;

/**
 * 远程调用异常
 *
 * @author quanchangnai
 */
public class CallException extends RuntimeException {

    private long callId;

    private String method;

    private boolean timeout;

    private String message;

    public CallException() {
    }

    public CallException(String message) {
        super(message);
    }

    public CallException(String message, boolean timeout) {
        super(message);
        this.timeout = timeout;
    }

    public CallException(Throwable cause) {
        super(cause);
    }

    public CallException(String message, Throwable cause) {
        super(message == null ? cause.toString() : message, cause);
    }


    protected void setCallId(long callId) {
        this.callId = callId;
    }

    protected void setMethod(String method) {
        this.method = method;
    }

    public boolean isTimeout() {
        return timeout;
    }

    @Override
    public String getMessage() {
        if (message != null) {
            return message;
        }

        message = "调用方法";

        if (timeout) {
            message += "超时";
        } else {
            message += "异常返回";
        }

        if (callId > 0) {
            message += ",callId:" + callId;
        }

        if (method != null) {
            message += ",方法:" + method;
        }

        if (!StringUtils.isBlank(super.getMessage())) {
            message += ",原因:" + super.getMessage();
        }

        return message;
    }

}
