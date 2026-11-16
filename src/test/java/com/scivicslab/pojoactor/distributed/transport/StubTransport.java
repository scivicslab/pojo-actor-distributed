package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.Receipt;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Test stub transport that records sent messages and returns a preset result. */
public class StubTransport implements TransportLayer {

    public final List<ActorMessage> sent = new ArrayList<>();
    public ActionResult presetResult = new ActionResult(true, "stub-result");
    public boolean accept = true;
    /** The first n sends are refused; later ones follow {@link #accept}. */
    public int refuseFirst = 0;
    public java.time.Duration lastTimeout;

    @Override
    public Receipt send(NodeInfo target, ActorMessage message) {
        sent.add(message);
        if (refuseFirst > 0) {
            refuseFirst--;
            return Receipt.refused(message.getMessageId(), "stub refused");
        }
        return accept ? Receipt.accepted(message.getMessageId())
                : Receipt.refused(message.getMessageId(), "stub refused");
    }

    @Override
    public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
        sent.add(message);
        lastTimeout = timeout;
        return presetResult;
    }

    @Override
    public void close() {}
}
