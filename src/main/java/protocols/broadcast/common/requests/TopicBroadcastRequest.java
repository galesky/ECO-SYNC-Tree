package protocols.broadcast.common.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class TopicBroadcastRequest extends ProtoRequest {

    public static final short REQUEST_ID = 220;

    private final Host sender;
    private final UUID msgId;
    private final Integer topic;

    private final byte[] msg;

    public TopicBroadcastRequest(UUID msgId, Host sender, Integer topic, byte[] msg) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.topic = topic;
        this.msg = msg;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getMsg() {
        return msg;
    }

    public Integer getTopic() { return topic; }

}
