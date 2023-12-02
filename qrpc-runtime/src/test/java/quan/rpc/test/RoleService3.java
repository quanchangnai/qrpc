package quan.rpc.test;

/**
 * @author quanchangnai
 */
public class RoleService3<A,B extends Runnable> extends RoleService2<B> {

    public RoleService3(long id) {
        super(id);
    }

    public String a2() {
        return "aaa";
    }
}
