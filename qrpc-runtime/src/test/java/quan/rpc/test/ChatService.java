package quan.rpc.test;

import quan.rpc.Endpoint;
import quan.rpc.Service;

/**
 * @author quanchangnai
 */
public  class ChatService extends Service<String> {

    @Override
    public String getId() {
        return "chat";
    }

    @Endpoint
    public void sendChatMsg(String msg) {
        logger.info("sendChatMsg:{}", msg);
    }


}
