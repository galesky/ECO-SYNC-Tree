package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class TopicSubMessage extends ProtoMessage {
    public static final short MSG_ID = 281;

    private final UUID mid;
    private final Host originalSender;
    private final int targetTopic;

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "mid=" + mid +
                ", sender=" + originalSender +
                ", targetTopic=" + targetTopic +
                '}';
    }

    public TopicSubMessage(UUID mid, Host originalSender, int targetTopic) {
        super(MSG_ID);
        this.mid = mid;
        this.originalSender = originalSender;
        this.targetTopic = targetTopic;
    }

	public Host getOriginalSender() {
        return originalSender;
    }

    public int getTargetTopic() { return targetTopic; }

    public UUID getMid() {
        return mid;
    }

    public static ISerializer<TopicSubMessage> serializer = new ISerializer<TopicSubMessage>() {
        @Override
        public void serialize(TopicSubMessage topicGossipMessage, ByteBuf out) throws IOException {
            out.writeLong(topicGossipMessage.mid.getMostSignificantBits());
            out.writeLong(topicGossipMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(topicGossipMessage.originalSender, out);
            out.writeInt(topicGossipMessage.targetTopic);
        }

        @Override
        public TopicSubMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int targetTopic = in.readInt();
            return new TopicSubMessage(mid, sender, targetTopic);
        }
    };

    public static TopicSubMessage deserialize(DataInputStream dis) throws IOException {
        long firstLong = dis.readLong();
        long secondLong = dis.readLong();
        UUID mid = new UUID(firstLong, secondLong);
        byte[] addrBytes = new byte[4];
        dis.read(addrBytes);
        int port = dis.readShort() & '\uffff';
        Host sender = new Host(InetAddress.getByAddress(addrBytes), port);
        int targetTopic = dis.readInt();
        return new TopicSubMessage(mid, sender, targetTopic);
    }
}
