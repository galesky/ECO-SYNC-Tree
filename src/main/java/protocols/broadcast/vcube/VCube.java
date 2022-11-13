package protocols.broadcast.vcube;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.GossipMessage;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.utils.CommunicationCostCalculator;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class VCube extends CommunicationCostCalculator {
    private static final Logger logger = LogManager.getLogger(VCube.class);

    public static final String PROTOCOL_NAME = "VCUBEPS";
    public static final short PROTOCOL_ID = 837;

    private int seqNumber; // Counter of local operations

    protected int channelId;
    private final static int PORT_MAPPING = 1000;

    private final HashSet<Host> neighborSet;
    private final HashSet<UUID> receivedMsgIds;

    private final Host myself;

    public VCube(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.neighborSet = new HashSet<>();
        this.receivedMsgIds = new HashSet<>();

        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("bcast_port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
        // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
        // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
        // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties
        /*--------------------- Register Request Handlers ----------------------------- */
        // these are requests that come from the CRDT app Layer to send to other neighbors
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Handlers -------------------------- */
        // Every gossip message is applied through this, this means both locally generated as received from other nodes
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossipMsg, this::onMessageFailed);

        /*---------------------- Register Message Serializers ---------------------- */
        // Note: Not sure if this is really needed. It is not directly called by this class but we do receive/send
        // Gossip messages
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
    }

    @Override
    public void init(Properties props) {

    }

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbor = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (neighborSet.add(neighbor)) {
            logger.debug("Added {} to partial view due to up {}", neighbor, neighborSet);
        } else {
            logger.error("Tried to add {} to partial view but is already there {}", neighbor, neighborSet);
        }

        openConnection(neighbor);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbor = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (neighborSet.remove(neighbor)) {
            logger.debug("Removed {} from neighbours due to down {}", neighbor, neighborSet);
        }

        closeConnection(neighbor);
    }

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.debug("Propagating my {} to {}", mid, neighborSet);
        GossipMessage msg = new GossipMessage(mid, myself, ++seqNumber, content);
        logger.info("SENT {}", mid);
        uponReceiveGossipMsg(msg, myself, getProtoId(), -1);
    }

    private void uponReceiveGossipMsg(GossipMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        logger.debug("Received {} from {}. Is from sync {}", mid, from, false);
        if (receivedMsgIds.add(mid)) {
            handleGossipMessage(msg, from, false);
        } else {
            logger.info("DUPLICATE from {}", from);
            // track stats here
        }
    }

    private void handleGossipMessage(GossipMessage msg, Host from, boolean fromSync) {
        Host sender = msg.getOriginalSender();

        UUID mid = msg.getMid();
        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, msg.getContent()));
        forwardGossipMessage(msg, from);
    }

    private void forwardGossipMessage(GossipMessage msg, Host from) {
        neighborSet.forEach(host -> {
            if (!host.equals(from)) {
                logger.debug("Sent {} to {}", msg, host);
                sendMessage(msg, host);
                // this.stats.incrementSentFlood();
            }
        });
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }
}
