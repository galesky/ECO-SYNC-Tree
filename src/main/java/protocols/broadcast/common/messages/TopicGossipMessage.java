package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.vcube.CausalBarrierItem;
import protocols.broadcast.vcube.VCube;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TopicGossipMessage extends ProtoMessage {

    private static final Logger logger = LogManager.getLogger(TopicGossipMessage.class);

    public static final short MSG_ID = 926;

    private final UUID mid;
    private final Host originalSender;
    private final int senderClock;
    private final byte[] content;

    private final int topic;

    private final List<CausalBarrierItem> cbList;

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "mid=" + mid +
                ", sender=" + originalSender +
                ", senderClock=" + senderClock +
                ", topic=" + topic +
                '}';
    }

    public static TopicGossipMessage from (TopicGossipMessage msg) {
        List<CausalBarrierItem> clonedList = new ArrayList<>(msg.cbList);
        return new TopicGossipMessage(
                msg.mid,
                msg.originalSender,
                msg.senderClock,
                msg.content,
                msg.topic,
                clonedList);
    }

    public TopicGossipMessage(UUID mid, Host originalSender, int senderClock, byte[] content, int topic, List<CausalBarrierItem> cbList) {
        super(MSG_ID);
        this.mid = mid;
        this.originalSender = originalSender;
        this.senderClock = senderClock;
        this.content = content;
        this.topic = topic;
        this.cbList = cbList;
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

    public List<CausalBarrierItem> getCausalBarrierList(){
        return cbList;
    }

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
            out.writeInt(topicGossipMessage.cbList.size());
            // logger.info("about to serialize CB List {}, size {}, from mid {}", topicGossipMessage.cbList, topicGossipMessage.cbList.size(), topicGossipMessage.getMid());
            for (CausalBarrierItem cb : topicGossipMessage.cbList) {
                out.writeInt(cb.getSource()); // source
                out.writeInt(cb.getCounter()); // counter
            }
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
            int listSize = in.readInt();
            List<CausalBarrierItem> newCbList = new ArrayList<>();
            // logger.info("about to de-serialize CB List {} from mid {}", listSize, mid);
            for (int i = 0; i < listSize; i++) {
                int source = in.readInt();
                int counter = in.readInt();
                // logger.info("de-serialize mid {} it {}", mid, i);
                newCbList.add(new CausalBarrierItem(source,counter));
            }
            // logger.info("de-serialize mid {} post cb is {}", mid, newCbList);

            return new TopicGossipMessage(mid, sender, senderClock, content, topic, newCbList);
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
        int listSize = dis.readInt();
        // logger.info("about to de-serialize CB List {} from mid {}", listSize, mid);
        List<CausalBarrierItem> newCbList = new ArrayList<>();
        for (int i = 0; i < listSize; i++) {
            int source = dis.readInt();
            int counter = dis.readInt();
            // logger.info("de-serialize mid {} it {}", mid, i);
            newCbList.add(new CausalBarrierItem(source,counter));
        }
        // logger.info("de-serialize mid {} post cb is {}", mid, newCbList);
        return new TopicGossipMessage(mid, sender, senderClock, content, topic, newCbList);
    }
}
