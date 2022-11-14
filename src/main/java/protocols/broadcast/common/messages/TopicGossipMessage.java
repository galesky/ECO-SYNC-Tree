package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class TopicGossipMessage extends ProtoMessage {
    public static final short MSG_ID = 926;

    private final UUID mid;
    private final Host originalSender;
    private final int senderClock;
    private final byte[] content;

    private final int topic;

    @Override
    public String toString() {
        return "TopicGossipMessage{" +
                "mid=" + mid +
                ", sender=" + originalSender +
                ", senderClock=" + senderClock +
                ", topic=" + topic +
                '}';
    }

    public TopicGossipMessage(UUID mid, Host originalSender, int senderClock, byte[] content, int topic) {
        super(MSG_ID);
        this.mid = mid;
        this.originalSender = originalSender;
        this.senderClock = senderClock;
        this.content = content;
        this.topic = topic;
    }

	public Host getOriginalSender() {
        return originalSender;
    }

    public int getSenderClock() {
        return senderClock;
    }

    public UUID getMid() {
        return mid;
    }

    public byte[] getContent() {
        return content;
    }

    public int getTopic() { return topic; }

    public static ISerializer<TopicGossipMessage> serializer = new ISerializer<TopicGossipMessage>() {
        @Override
        public void serialize(TopicGossipMessage topicGossipMessage, ByteBuf out) throws IOException {
            out.writeLong(topicGossipMessage.mid.getMostSignificantBits());
            out.writeLong(topicGossipMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(topicGossipMessage.originalSender, out);
            out.writeInt(topicGossipMessage.senderClock);
            out.writeInt(topicGossipMessage.content.length);
            if (topicGossipMessage.content.length > 0) {
                out.writeBytes(topicGossipMessage.content);
            }
            out.writeInt(topicGossipMessage.topic);
        }

        @Override
        public TopicGossipMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);
            int topic = in.readInt();

            return new TopicGossipMessage(mid, sender, senderClock, content, topic);
        }
    };

    public static TopicGossipMessage deserialize(DataInputStream dis) throws IOException {
        long firstLong = dis.readLong();
        long secondLong = dis.readLong();
        UUID mid = new UUID(firstLong, secondLong);
        byte[] addrBytes = new byte[4];
        dis.read(addrBytes);
        int port = dis.readShort() & '\uffff';
        Host sender = new Host(InetAddress.getByAddress(addrBytes), port);
        int senderClock = dis.readInt();
        int size = dis.readInt();
        byte[] content = new byte[size];
        if (size > 0)
            dis.read(content);
        int topic = dis.readInt();
        return new TopicGossipMessage(mid, sender, senderClock, content, topic);
    }
}
