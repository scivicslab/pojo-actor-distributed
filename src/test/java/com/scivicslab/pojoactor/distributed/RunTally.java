package com.scivicslab.pojoactor.distributed;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;

/**
 * Test fixture: counts how many times each token was recorded. The integration tests send
 * {@code record} with a token of their own over Kafka and read the count to tell a message that
 * ran once from one that ran twice or never.
 */
public class RunTally implements CallableByActionName {

    private final ConcurrentHashMap<String, AtomicInteger> runs = new ConcurrentHashMap<>();

    /** Records one run of the token and returns how many runs it has now. */
    public int record(String token) {
        return runs.computeIfAbsent(token, t -> new AtomicInteger()).incrementAndGet();
    }

    /** How many times the token ran; 0 when it never did. */
    public int runs(String token) {
        AtomicInteger n = runs.get(token);
        return n == null ? 0 : n.get();
    }

    /** Every token and its count, sorted by token. */
    public Map<String, Integer> snapshot() {
        Map<String, Integer> copy = new TreeMap<>();
        runs.forEach((k, v) -> copy.put(k, v.get()));
        return copy;
    }

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        if ("record".equals(actionName)) {
            return new ActionResult(true, String.valueOf(record(args)));
        }
        return new ActionResult(false, "Unknown action: " + actionName);
    }
}
