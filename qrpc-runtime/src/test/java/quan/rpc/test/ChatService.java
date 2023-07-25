package quan.rpc.test;

import quan.rpc.Endpoint;
import quan.rpc.Service;

/**
 * @author quanchangnai
 */
@Service.Singleton(id = "chat")
public class ChatService extends Service {

//    @Override
//    public Object getId() {
//        return super.getId();
//    }

    @Endpoint
    public void sendChatMsg(String msg) {
        logger.info("sendChatMsg:{}", msg);
    }

}
