package quan.rpc.protocol;

import quan.rpc.serialize.ObjectReader;
import quan.rpc.serialize.ObjectWriter;

/**
 * 握手协议
 *
 * @author quanchangnai
 */
public class Handshake extends Protocol {

    private String ip;

    private int port;

    protected Handshake() {
    }


    public Handshake(int serverId, String ip, int port) {
        super(serverId);
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
    public void transferTo(ObjectWriter writer) {
        super.transferTo(writer);
        writer.write(ip);
        writer.write(port);
    }

    @Override
    public void transferFrom(ObjectReader reader) {
        super.transferFrom(reader);
        ip = reader.read();
        port = reader.read();
    }


    @Override
    public String toString() {
        return "Handshake{" +
                "serverId='" + getServerId() + '\'' +
                "ip='" + ip + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
