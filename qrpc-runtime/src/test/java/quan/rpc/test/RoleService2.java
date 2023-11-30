package quan.rpc.test;

import quan.message.Message;
import quan.rpc.Endpoint;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * 角色服务
 */
public class RoleService2<T extends Object & Runnable> extends RoleService1 {

    private long id;

    private T t;

    public RoleService2(long id) {
        this.id = id;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public int getLevel() {
        return super.getLevel();
    }

    @Endpoint
    public int login1(int a, Integer b, Long... ll) {
        int r = a + b;
        logger.info("Execute RoleService1:{}.login1({},{},{})={} at Worker:{}", id, a, b, Arrays.toString(ll), r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public int login2(Integer a, Integer b, Message... messages) {
        int r = a + b;
        logger.info("Execute RoleService1:{}.login2({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public int login3(Integer a, Integer b, int[][] cc, String... ss) {
        int r = a + b;
        logger.info("Execute RoleService1:{}.login3({},{})={} at Worker:{}", id, a, b, r, this.getWorker().getId());
        return r;
    }

    @Endpoint
    public void login4(String[] ss) {
        logger.info("login4:{}", Arrays.toString(ss));
    }

    @Endpoint
    public void login5(Object... oo) {
        logger.info("login5:{}", Arrays.toString(oo));
    }

    /**
     * 角色登陆1
     */
    @Endpoint
    public void logout(T t,int a) {
        this.t = t;
        logger.info("logout1:{}", t);
    }

    @Endpoint
    public <T> void logout(T t,String s) {
        logger.info("logout2:{}", t);
    }

    @Endpoint
    public <R extends Runnable> R logout(T t, List<T> list) {
        this.t = t;
        logger.info("logout3:{}", t);
        return null;
    }

    @Endpoint(safeArgs = true)
    public int count(Set<Integer> set) {
        return set.size();
    }

}
