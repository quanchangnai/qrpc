package quan.rpc.test;

import quan.rpc.Endpoint;
import quan.rpc.Service;

/**
 * @author quanchangnai
 */
@Service.Single(id = "chat")
public class ChatService extends Service {

    @Endpoint
    public void sendChatMsg(String msg) {

    }

}
