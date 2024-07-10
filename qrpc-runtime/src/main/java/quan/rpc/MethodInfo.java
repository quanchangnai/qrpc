package quan.rpc;

/**
 * 服务方法信息，在执行RPC时描述目标方法
 *
 * @author quanchangnai
 */
public class MethodInfo {

    /**
     * 方法ID
     */
    private int id;

    /**
     * 方法标签
     */
    private String label;


    /**
     * 方法的所有参数都是安全的
     *
     * @see Endpoint#safeArgs()
     */
    private boolean safeArgs;

    /**
     * 方法的返回结果是安全的
     *
     * @see Endpoint#safeReturn()
     */
    private boolean safeReturn;

    /**
     * 方法的过期时间(秒)
     */
    private int expiredTime;


    public MethodInfo(int id, String label, boolean safeArgs, boolean safeReturn, int expiredTime) {
        this.id = id;
        this.label = label;
        this.safeArgs = safeArgs;
        this.safeReturn = safeReturn;
        this.expiredTime = expiredTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isSafeArgs() {
        return safeArgs;
    }

    public void setSafeArgs(boolean safeArgs) {
        this.safeArgs = safeArgs;
    }

    public boolean isSafeReturn() {
        return safeReturn;
    }

    public void setSafeReturn(boolean safeReturn) {
        this.safeReturn = safeReturn;
    }

    public int getExpiredTime() {
        return expiredTime;
    }

    public void setExpiredTime(int expiredTime) {
        this.expiredTime = expiredTime;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                ", label='" + label + '\'' +
                ", safeArgs=" + safeArgs +
                ", safeReturn=" + safeReturn +
                ", expiredTime=" + expiredTime +
                '}';
    }

}
