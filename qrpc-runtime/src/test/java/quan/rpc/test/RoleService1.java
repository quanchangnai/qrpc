package quan.rpc.test;

import quan.rpc.Endpoint;
import quan.rpc.Service;

/**
 * @author quanchangnai
 */
public abstract class RoleService1 extends Service<Long> {

    @Override
    public Long getId() {
        return null;
    }

    @Endpoint
    public String getName() {
        return "aaa";
    }

    @Endpoint
    public int getLevel() {
        return 1;
    }


}
