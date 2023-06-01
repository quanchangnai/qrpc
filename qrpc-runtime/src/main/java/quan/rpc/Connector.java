package quan.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quan.rpc.protocol.Protocol;
import quan.rpc.protocol.Request;
import quan.rpc.protocol.Response;

/**
 * 网络连接器
 *
 * @author quanchangnai
 */
public abstract class Connector {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Node node;

    protected abstract void start();

    protected abstract void stop();

    protected abstract boolean isLegalRemote(int remoteId);

    protected abstract void sendProtocol(int remoteId, Protocol protocol);

    protected void handleProtocol(Protocol protocol) {
        if (protocol instanceof Request) {
            node.handleRequest((Request) protocol);
        } else if (protocol instanceof Response) {
            node.handleResponse((Response) protocol);
        } else {
            logger.error("收到非法RPC协议:{}", protocol);
        }
    }

}
