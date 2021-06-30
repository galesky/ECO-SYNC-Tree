package protocols.broadcast.flood;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SyncOpsMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.flood.messages.FloodMessage;
import protocols.broadcast.common.notifications.SendVectorClockNotification;
import protocols.broadcast.common.notifications.VectorClockNotification;
import protocols.broadcast.common.requests.SyncOpsRequest;
import protocols.broadcast.common.requests.VectorClockRequest;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class FloodBroadcast extends GenericProtocol  {
    private static final Logger logger = LogManager.getLogger(FloodBroadcast.class);

    public final static short PROTOCOL_ID = 1500;
    public final static String PROTOCOL_NAME = "Flood";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;

    private final long reconnectTimeout;

    private final Set<Host> partialView;
    private final Set<Host> neighbours;
    private final Queue<Host> pending;
    private Host currentPending;
    private final Queue<FloodMessage> bufferedOps; //Buffer ops received between sending vc to kernel and sending sync ops (and send them after)
    private boolean buffering; //To know if we are between sending vc to kernel and sending ops to neighbour
    private final Set<UUID> received;

    /*--------------------------------- Initialization ---------------------------------------- */

    public FloodBroadcast(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));
        this.partialView = new HashSet<>();
        this.neighbours = new HashSet<>();
        this.pending = new LinkedList<>();
        this.bufferedOps = new LinkedList<>();
        this.buffering = false;
        this.received = new HashSet<>();

        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to setup channel-specific properties. See the
        // channel description for more details.
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

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(VectorClockRequest.REQUEST_ID, this::uponVectorClock);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOps);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, FloodMessage.MSG_ID, FloodMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, FloodMessage.MSG_ID, this::uponReceiveFlood, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponReceiveSyncOps, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        Host sender = request.getSender();
        byte[] content = request.getMsg();
        logger.debug("Propagating my {} to {}", mid, neighbours);
        FloodMessage msg = new FloodMessage(mid, sender, sourceProto, content);
        uponReceiveFlood(msg, myself, getProtoId(), -1);
    }

    private void uponVectorClock(VectorClockRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(), request.getVectorClock());
        sendMessage(msg, neighbour, TCPChannel.CONNECTION_IN);
        logger.info("Sent {} to {}", msg, neighbour);
    }

    private void uponSyncOps(SyncOpsRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        if(!neighbour.equals(currentPending))
            return;

        SyncOpsMessage msg = new SyncOpsMessage(request.getMsgId(), request.getIds(), request.getOperations());
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
        handleBufferedOperations(neighbour);
        addPendingToNeighbours();
    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveFlood(FloodMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        if (received.add(mid)) {
            logger.info("Received op {} from {}", mid, from);
            if(buffering)
                this.bufferedOps.add(msg);

            triggerNotification(new DeliverNotification(mid, msg.getSender(), msg.getContent(), false));
            forwardFloodMessage(msg, from);
        }
    }

    private void forwardFloodMessage(FloodMessage msg, Host from) {
        neighbours.forEach(host -> {
            if (!host.equals(from)) {
                logger.trace("Sent {} to {}", msg, host);
                sendMessage(msg, host);
            }
        });
    }

    private void uponReceiveVectorClock(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(!from.equals(currentPending))
            return;
        this.buffering = true;
        triggerNotification(new VectorClockNotification(msg.getSender(), msg.getVectorClock()));
    }

    private void uponReceiveSendVectorClock(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        triggerNotification(new SendVectorClockNotification(msg.getSender()));
    }

    private void uponReceiveSyncOps(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        Iterator<byte[]> opIt = msg.getOperations().iterator();
        Iterator<byte[]> idIt = msg.getIds().iterator();

        while (opIt.hasNext() && idIt.hasNext()) {
            byte[] serOp = opIt.next();
            byte[] serId = idIt.next();
            UUID mid = deserializeId(serId);

            if (received.add(mid)) {
                triggerNotification(new DeliverNotification(mid, from, serOp, true));
                logger.info("Received sync op {} from {}", mid, from);
                forwardFloodMessage(new FloodMessage(mid, from , sourceProto, serOp), from);
            }
        }
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponReconnectTimeout(ReconnectTimeout timeout, long timerId) {
        Host neighbour = timeout.getHost();
        if(partialView.contains(neighbour)) {
            logger.info("Reconnecting with {}", neighbour);
            openConnection(neighbour);
        } else {
            logger.info("Not reconnecting because {} is down", neighbour);
        }
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour)) {
            logger.info("Added {} to partial view due to up {}", neighbour, partialView);
        }

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.remove(neighbour)) {
            logger.info("Removed {} from partial view due to down {}", neighbour, partialView);
        }

        if (neighbours.remove(neighbour)) {
            logger.info("Removed {} from neighbours due to down {}", neighbour, neighbours);
        }

        if (pending.remove(neighbour)) {
            logger.info("Removed {} from pending due to down {}", neighbour, pending);
        }

        if (neighbour.equals(currentPending)) {
            logger.info("Removed {} from current pending due to down", neighbour);
            tryNextSync();
        }
        closeConnection(neighbour);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startSynchronization(Host neighbour) {
        if (currentPending == null) {
            currentPending = neighbour;
            logger.info("{} is my currentPending start", neighbour);
            requestVectorClock(currentPending);
        } else {
            pending.add(neighbour);
            logger.info("Added {} to pending {}", neighbour, pending);
        }
    }

    private void addPendingToNeighbours() {
        if (neighbours.add(currentPending)) {
            logger.info("Added {} to neighbours {} : pending list {}", currentPending, neighbours, pending);
        }

        tryNextSync();
    }

    private void tryNextSync() {
        currentPending = pending.poll();
        if (currentPending != null) {
            logger.info("{} is my currentPending try", currentPending);
            requestVectorClock(currentPending);
        }
    }

    private void requestVectorClock(Host neighbour) {
        SendVectorClockMessage msg = new SendVectorClockMessage(UUID.randomUUID(), myself);
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
    }

    private void handleBufferedOperations(Host neighbour) {
        this.buffering = false;
        FloodMessage msg;
        while((msg = bufferedOps.poll()) != null) {
            if(!msg.getSender().equals(neighbour)) {
                sendMessage(msg, neighbour);
                logger.info("Sent buffered {} to {}", msg, neighbour);
            }
        }
    }

    private UUID deserializeId(byte[] msg) {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        return new UUID(buf.readLong(), buf.readLong());
    }

    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.info("Host {} is down, cause: {}", host, event.getCause());

        if (neighbours.remove(host)) {
            logger.info("Removed {} from neighbours due to plumtree down {}", host, neighbours);
        }

        if (pending.remove(host)) {
            logger.info("Removed {} from pending due to plumtree down {}", host, pending);
        }

        if (host.equals(currentPending)) {
            logger.info("Removed {} from current pending due to plumtree down", host);
            tryNextSync();
        }
        setupTimer(new ReconnectTimeout(host), reconnectTimeout);
    }

    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.info("Connection to host {} failed, cause: {}", host, event.getCause());
        setupTimer(new ReconnectTimeout(host), reconnectTimeout);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);
        if(partialView.contains(neighbour)) {
            logger.info("Trying sync from neighbour {} up", neighbour);
            startSynchronization(neighbour);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }
}
