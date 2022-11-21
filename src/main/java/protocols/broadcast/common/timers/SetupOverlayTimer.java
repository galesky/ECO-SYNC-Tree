package protocols.broadcast.common.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SetupOverlayTimer extends ProtoTimer {
    public static final short TIMER_ID = 402;

    public SetupOverlayTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
