package quan.rpc;

import java.util.Arrays;

/**
 * RPC协议
 */
public abstract class Protocol {

    /**
     * 来源节点ID
     */
    private int originNodeId;

    public Protocol() {
    }

    public Protocol(int originNodeId) {
        this.originNodeId = originNodeId;
    }

    /**
     * @see #originNodeId
     */
    public int getOriginNodeId() {
        return originNodeId;
    }


    /**
     * 调用请求协议
     */
    public static class Request extends Protocol {

        /**
         * 调用ID
         */
        private long callId;

        /**
         * 目标服务ID
         */
        private Object serviceId;

        /**
         * 目标方法ID
         */
        private int methodId;

        /**
         * 目标方法参数
         */
        private Object[] params;

        /**
         * 过期时间
         */
        private long expiredTime;

        protected Request() {
        }

        public Request(int originNodeId, long callId, Object serviceId, int methodId, Object... params) {
            super(originNodeId);
            this.callId = callId;
            this.serviceId = serviceId;
            this.methodId = methodId;
            this.params = params;
        }

        public long getCallId() {
            return callId;
        }

        public Object getServiceId() {
            return serviceId;
        }

        public int getMethodId() {
            return methodId;
        }

        public Object[] getParams() {
            return params;
        }

        public long getExpiredTime() {
            return expiredTime;
        }

        public void setExpiredTime(long expiredTime) {
            this.expiredTime = expiredTime;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "originNodeId='" + getOriginNodeId() + '\'' +
                    ", callId=" + callId +
                    ", serviceId=" + serviceId +
                    ", methodId=" + methodId +
                    ", expiredTime=" + expiredTime +
                    ", params=" + Arrays.toString(params) +
                    '}';
        }

    }

    /**
     * 调用响应协议
     */
    public static class Response extends Protocol {

        /**
         * 调用ID
         */
        private long callId;

        /**
         * 返回的结果
         */
        private Object result;

        private String exception;

        protected Response() {
        }

        public Response(int originNodeId, long callId, Object result, String exception) {
            super(originNodeId);
            this.callId = callId;
            this.result = result;
            this.exception = exception;
        }

        public long getCallId() {
            return callId;
        }

        public Object getResult() {
            return result;
        }

        public String getException() {
            return exception;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "originNodeId='" + getOriginNodeId() + '\'' +
                    ", callId=" + callId +
                    ", result=" + result +
                    ", exception='" + exception + '\'' +
                    '}';
        }

    }


    public static class PingPong extends Protocol {

        private long time;

        protected PingPong() {
        }

        public PingPong(int originNodeId, long time) {
            super(originNodeId);
            this.time = time;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        @Override
        public String toString() {
            return "PingPong{" +
                    "originNodeId='" + getOriginNodeId() + '\'' +
                    ", time=" + time +
                    '}';
        }

    }

    /**
     * 握手协议
     */
    public static class Handshake extends Protocol {

        private String ip;

        private int port;

        protected Handshake() {
        }


        public Handshake(int originNodeId, String ip, int port) {
            super(originNodeId);
            this.ip = ip;
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return "Handshake{" +
                    "originNodeId='" + getOriginNodeId() + '\'' +
                    ", ip='" + ip + '\'' +
                    ", port='" + port + '\'' +
                    '}';
        }
    }
}
