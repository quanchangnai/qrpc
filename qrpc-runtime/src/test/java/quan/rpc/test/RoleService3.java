package quan.rpc.test;

import quan.rpc.Endpoint;

/**
 * @author quanchangnai
 */
public class RoleService3<A,B extends Runnable> extends RoleService2<B> {

    public RoleService3(long id) {
        super(id);
    }

    @Endpoint
    public String a2() {
        return "aaa";
    }
}
