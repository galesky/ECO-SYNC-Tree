package protocols.membership.hyparview.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class View implements IView {

    private static final Logger logger = LogManager.getLogger(View.class);


    private final int capacity;
    private final Set<Host> peers;
    private final Random rnd;
    private final Host self;
    private boolean isActive;

    private IView other;
    private Set<Host> pending;

    public View(boolean isActive, int capacity, Host self, Random rnd) {
        this.capacity = capacity;
        this.self = self;
        this.peers = new HashSet<>();
        this.rnd = rnd;
        this.isActive = isActive;
    }

    public void setOther(IView other, Set<Host> pending) {
        this.other = other;
        this.pending = pending;
    }

    @Override
    public String toString() {
        return "View{" +
                "peers=" + peers +
                '}';
    }

    public Host addPeer(Host peer) {
        if(!peer.equals(self) && !peers.contains(peer) && !other.containsPeer(peer) && !pending.contains(peer)) {
            Host excess = null;
            if (peers.size() == capacity)
                excess = dropRandom();
            boolean ret = peers.add(peer);
            logger.debug("Added {} {} {}", peer, isActive, peers);
            assert ret;
            assert peers.size() <= capacity;
            return excess;
        }
        return null;
    }

    public boolean removePeer(Host peer) {
        boolean removed = peers.remove(peer);
        if(removed)
            logger.debug("Removed {} {} {}", peer, isActive, peers);
        return removed;
    }

    public boolean containsPeer(Host peer) {
        return peers.contains(peer);
    }

    public Host dropRandom() {
        Host torm = null;
        if(peers.size() > 0) {
            int idx = rnd.nextInt(peers.size());
            Host[] hosts = peers.toArray(new Host[0]);
            torm = hosts[idx];
            peers.remove(torm);
            logger.debug("Removed {} {} {}", torm, isActive, peers);
        }
        return torm;
    }

    public Set<Host> getRandomSample(int sampleSize) {
        Set<Host> toret;
        if(peers.size() > sampleSize) {
            List<Host> hosts = new ArrayList<>(peers);
            while (hosts.size() > sampleSize)
                hosts.remove(rnd.nextInt(hosts.size()));
            toret = new HashSet<>(hosts);
        } else
            toret = peers;

        return toret;
    }

    public Set<Host> getPeers() {
        return peers;
    }

    public Host getRandom() {
        if(peers.size() > 0) {
            int idx = rnd.nextInt(peers.size());
            Host[] hosts = peers.toArray(new Host[0]);
            return hosts[idx];
        } else
            return null;
    }

    public Host getRandomDiff(Host from) {
        List<Host> hosts = new ArrayList<>(peers);
        hosts.remove(from);
        if(hosts.size() > 0)
            return hosts.get(rnd.nextInt(hosts.size()));
        else
            return null;
    }

    public boolean fullWithPending(Set<Host> pending) {
        assert  peers.size() + pending.size() <= capacity;
        return peers.size() + pending.size() >= capacity;
    }

    public boolean isFull() {
        return peers.size() >= capacity;
    }

    public boolean isEmpty() {
        return peers.isEmpty();
    }

}
