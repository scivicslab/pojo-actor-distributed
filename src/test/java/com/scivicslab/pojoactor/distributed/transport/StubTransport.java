package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Test stub transport that records sent messages and returns a preset result. */
public class StubTransport implements TransportLayer {

    public final List<ActorMessage> sent = new ArrayList<>();
    public ActionResult presetResult = new ActionResult(true, "stub-result");

    @Override
    public void send(NodeInfo target, ActorMessage message) {
        sent.add(message);
    }

    @Override
    public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
        sent.add(message);
        return presetResult;
    }

    @Override
    public void close() {}
}
