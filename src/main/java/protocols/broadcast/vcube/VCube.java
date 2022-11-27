package protocols.broadcast.vcube;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.timers.CreateCRDTsTimer;
import protocols.broadcast.common.messages.TopicGossipMessage;
import protocols.broadcast.common.messages.TopicSubMessage;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.requests.TopicBroadcastRequest;
import protocols.broadcast.common.timers.SetupOverlayTimer;
import protocols.broadcast.common.utils.CommunicationCostCalculator;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class VCube extends CommunicationCostCalculator {
    private static final Logger logger = LogManager.getLogger(VCube.class);

    public static final String PROTOCOL_NAME = "VCUBEPS";
    public static final short PROTOCOL_ID = 837;

    private final int createTime;
    private static final int TO_MILLIS = 1000;
    private int seqNumber; // Counter of local operations

    private final Map<Integer, HashSet<Integer>> vcubeConfig = VCubeConfig.nodeIdsByTopic;

    protected int channelId;
    private final static int PORT_MAPPING = 1000;

    private final HashSet<Host> neighborSet;
    private final HashSet<UUID> receivedMsgIds;

    private final HashMap<Integer, Host> hostByNodeId;
    private final HashMap<Integer, HashSet<Integer>> nodeIdsByTopic;
    private final HashSet<Integer> myTopics;
    private final Host myself;
    private final int myId;
    /**
     * The height of the tree. Also matches the max number of clusters that a tree will broadcast to.
     */
    private int dimension = -1;

    public VCube(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        this.myId = idFromHostAddress(myself);

        this.createTime = Integer.parseInt(properties.getProperty("create_time"));
        logger.info("Setup create time as {}", createTime);

        this.neighborSet = new HashSet<>();
        this.receivedMsgIds = new HashSet<>();
        this.hostByNodeId = new HashMap<>();
        this.nodeIdsByTopic = new HashMap<>();
        this.myTopics = new HashSet<>();

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
        registerRequestHandler(TopicBroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Handlers -------------------------- */
        // Every gossip message is applied through this, this means both locally generated as received from other nodes
        registerMessageHandler(channelId, TopicGossipMessage.MSG_ID, this::uponReceiveGossipMsg, this::onMessageFailed);
        registerMessageHandler(channelId, TopicSubMessage.MSG_ID, this::uponReceiveSubMsg, this::onMessageFailed);


        /*---------------------- Register Message Serializers ---------------------- */
        // Note: Not sure if this is really needed. It is not directly called by this class but we do receive/send
        // Gossip messages
        registerMessageSerializer(channelId, TopicGossipMessage.MSG_ID, TopicGossipMessage.serializer);
        registerMessageSerializer(channelId, TopicSubMessage.MSG_ID, TopicSubMessage.serializer);
        /*---------------------- Register Timers ---------------------- */
        registerTimerHandler(SetupOverlayTimer.TIMER_ID, this::uponSetupOverlayTimer);

        /*---------------------- Register Channel events ---------------------- */
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        setupTimer(new SetupOverlayTimer(), (long) Math.ceil(createTime * TO_MILLIS * 0.7                                         ));
        setupMyTopics();
    }

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbor = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (neighborSet.add(neighbor)) {
            int neighborId = idFromHostAddress(neighbor);
            hostByNodeId.put(neighborId, neighbor);

            logger.info("Added {} with id {} to partial view due to up. Set is {} and map is {}", neighbor, neighborId, neighborSet, hostByNodeId);
        } else {
            logger.error("Tried to add {} to partial view but is already there {}", neighbor, neighborSet);
        }

        openConnection(neighbor);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbor = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (neighborSet.remove(neighbor)) {
            logger.info("Removed {} from neighbours due to down {}", neighbor, neighborSet);
        }

        closeConnection(neighbor);
    }

    private void uponBroadcastRequest(TopicBroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.info("Propagating my {} to {}", mid, neighborSet);
        // at this point we will likely decide to which topic to send the message to given a hot-topic distribution.
        TopicGossipMessage msg = new TopicGossipMessage(mid, myself, ++seqNumber, content, request.getTopic());
        logger.info("SENT {}", mid);
        uponReceiveGossipMsg(msg, myself, getProtoId(), -1);
    }

    private void uponReceiveGossipMsg(TopicGossipMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        int topic = msg.getTopic();
        logger.info("Received {} from {}. Topic is {}", mid, from, topic);
        if (receivedMsgIds.add(mid)) {
            logger.info("Total unique SUB messages so far {}", receivedMsgIds.size());
            handleTopicGossipMessage(msg, from);
        } else {
            logger.info("DUPLICATE from {}", from);
            // track stats here
        }
    }

    private void handleTopicGossipMessage(TopicGossipMessage msg, Host from) {
        Host sender = msg.getOriginalSender();

        UUID mid = msg.getMid();
        logger.info("RECEIVED TopicGossip {}", mid);
        triggerNotification(new DeliverNotification(mid, msg.getContent()));
        forwardTopicGossipMessage(msg, from);
    }

    private void forwardTopicGossipMessage(TopicGossipMessage msg, Host from) {
        int cluster = myself.equals(from) ? getDimension() : (cluster(myId, idFromHostAddress(from)) - 1);
        List<Integer> neighbors = hypercubeNeighborhood(myId, cluster, msg.getTopic());

        logger.info("Determined HypercubeNeighbors {}", neighbors);
        neighbors.forEach(hostId -> {
            Host host = hostByNodeId.get(hostId);
            logger.info("Select host {} with id {}", host, hostId);

            if (!host.equals(from)) {
                logger.info("Sent {} to {}", msg, host);
                sendMessage(msg, host);
                // this.stats.incrementSentFlood();
            }
        });
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }

    // --------- SUB/UNS message handling ------
    private void uponSetupOverlayTimer(SetupOverlayTimer timer, long timerId) {
        logger.info("Starting uponSetupOverlayTimer");
        for (int topic: myTopics) {
            UUID uuid = UUID.randomUUID();
            TopicSubMessage msg = new TopicSubMessage(uuid, myself,topic);
            forwardTopicSubMessage(msg, myself);
        }
    }

    private void setupMyTopics() {
        vcubeConfig.forEach((key, value) -> {
            // only initialize myself
            if (value.contains(myId)) {
                nodeIdsByTopic.put(key, new HashSet<>(Arrays.asList(myId)));
                myTopics.add(key);
            }
        });
    }

    private void uponReceiveSubMsg(TopicSubMessage msg, Host from, short sourceProto, int channelId) {
        UUID mid = msg.getMid();
        int targetTopic = msg.getTargetTopic();
        logger.info("Received SUB message {} from {}. Target topic is {}", mid, from, targetTopic);
        if (receivedMsgIds.add(mid)) {
            logger.info("Total unique SUB messages so far {}", receivedMsgIds.size());
            handleTopicSubMessage(msg, from);
        } else {
            logger.info("DUPLICATE SUB message from {}", from);
            // track stats here
        }
    }

    private void handleTopicSubMessage(TopicSubMessage msg, Host from) {
        Host sender = msg.getOriginalSender();
        UUID mid = msg.getMid();
        int targetTopic = msg.getTargetTopic();
        // This I'm not sure.
        // Should we only keep track of subscribers from topics we are members or of all ?
        if (myTopics.contains(targetTopic)) {
            nodeIdsByTopic.get(targetTopic).add(idFromHostAddress(sender));
        }
        logger.info("RECEIVED {} from {} sender", mid, sender);
        // add to topic sub list
        forwardTopicSubMessage(msg, from);
    }

    private void forwardTopicSubMessage(TopicSubMessage msg, Host from) {
        // dryrun hypercube
        List<Integer> hypercubeNeighborhood = hypercubeNeighborhood(myId, getDimension(), null);
        logger.info("Determined hypercubeNeighbors {}", hypercubeNeighborhood);
        neighborSet.forEach(host -> {
            if (!host.equals(from)) {
                logger.info("Sent {} to {}", msg, host);
                sendMessage(msg, host);
                // this.stats.incrementSentFlood();
            }
        });
    }

    // --------- From VCube PS -----------------

    /**
     * Gets the dimension of the tree considering the number of nodes.
     * It adds 1 to the computed height since we are assuming that log2(size) is not an integer.
     * @return
     */
    public int getDimension() {

        if (this.dimension == -1) {

            this.dimension = (int) (Math.log10(neighborSet.size()) / Math.log10(2)) + 1;
            logger.info("  dimension has been set to {} since there are {} neighbors", dimension, neighborSet.size());

        }

        return this.dimension;
    }

    public static int idFromHostAddress(Host host) {
        // offset ip by -10 since our ip range starts at 10 (from config file)
        int id = Integer.parseInt(host.getAddress().getHostAddress().split("\\.")[3]) - 10;
        logger.info("Getting id from host {} got {}", host.getAddress().getHostAddress(), id);
        return id;
    }

    /*
     * Returns the set of all processes that are virtually
     * connected to process i.
     *
     * neighborhood_i(h) = {j | j = FF_neighbor_i(s),
     * j != null, 1 <= s <= h, h <= log2(n)}
     */
    public List<Integer> hypercubeNeighborhood(int i, int h, Integer topic) {

        List<Integer> hypercubeNeighbors = new ArrayList<>();

        for (int s = 1; s <= h; s++) {

            Integer firstFaultFreeNeighbor = this.firstFaultFreeNeighbor(i, s, topic);

            if (firstFaultFreeNeighbor != null) {
                hypercubeNeighbors.add(firstFaultFreeNeighbor);
            }

        }
        return hypercubeNeighbors;

    }

    /*
     *  Returns the first fault-free node j in
     *  the cluster s of node i (c(i, s))
     */
    public Integer firstFaultFreeNeighbor(int i, int s, Integer topic) {

        List<Integer> cluster = new LinkedList<>();

        // side-effect: updates this.cluster adding neighbors
        this.clusterByNodeAndRound(cluster, i, s);

        if ((topic != null) && ! this.nodeIdsByTopic.containsKey(topic)) {
            this.nodeIdsByTopic.put(topic, new HashSet<>());
        }

        do {

            int k = cluster.remove(0);

            if (topic == null) {
                return k; // for SUB messages there is no topic metadata so we want to send to the first node
            }

            Integer match = null;

            if (this.nodeIdsByTopic.containsKey(topic)) {
                match = this.matchView(this.nodeIdsByTopic.get(topic), k);
            }

            if (match != null) {
                return k;
            }

        } while (! cluster.isEmpty());

        return null;
    }

    /*
     * Determines the cluster tested by node i during the round s
     *
     * c_(i,s) = { i xor 2^(s-1), c_(i xor 2^(s-1), 1), ..., c_(i xor 2^(s-1), (s-1)) }
     * a.k.a cis(i,s)
     */
    private void clusterByNodeAndRound(List<Integer> cluster, int node_i, int round_s) {

        int xor = node_i ^ (int) Math.pow(2, (round_s - 1));

        cluster.add(xor);

        for (int j = 1; j < round_s; j++) {
            // Recursively calls cis until round == 1
            this.clusterByNodeAndRound(cluster, xor, j);

        }
    }

    /**
     * This version is simpler than the one found on the Original VCube-PS implementation since it does
     * not consider:
     *  - Joining and leaving dynamics
     *  - Presence of explicit FORWARDER nodes
     * I think it is trying to decide it a node from the cluster (list of nodes) is a subscriber to the topic.
     * @param topicNeighbors
     * @param neighborId
     * @return
     */
    public Integer matchView(HashSet<Integer> topicNeighbors, Integer neighborId) {

        if (topicNeighbors == null || !topicNeighbors.contains(neighborId)) {
            return null;
        }
        return neighborId; // If got here, then it contains this node
    }

    /*
     * Returns the index s of the cluster of
     * process i that contains process j.
     *
     * cluster_i(j) = s (msb(i xor j) + 1)
     */
    public int cluster(int node_i, int node_j) {

        int s = 0;

        for (int k = node_i ^ node_j; k > 0; k = k >> 1) {
            s++;
        }

        return s;

    }

}
