package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;

/**
 * Test fixture: takes a request, does the work, and sends the answer back as a message of its own,
 * the way a long request is answered over Kafka (there is no synchronous call there).
 *
 * <p>{@code square} takes {@code "token,n"} and sends {@code answer} {@code "token,n*n"} to the
 * actor it was given.</p>
 */
public class Squarer implements CallableByActionName {

    private final RemoteActorRef answerTo;
    private final String answerAction;

    /**
     * @param answerTo     the remote actor that receives the answers
     * @param answerAction the action on it that receives an answer
     */
    public Squarer(RemoteActorRef answerTo, String answerAction) {
        this.answerTo = answerTo;
        this.answerAction = answerAction;
    }

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        if (!"square".equals(actionName)) {
            return new ActionResult(false, "Unknown action: " + actionName);
        }
        String[] p = args.split(",");
        long n = Long.parseLong(p[1].trim());
        Receipt receipt = answerTo.send(answerAction, p[0].trim() + "," + (n * n));
        return new ActionResult(receipt.accepted(), receipt.accepted() ? "answer sent" : receipt.detail());
    }
}
