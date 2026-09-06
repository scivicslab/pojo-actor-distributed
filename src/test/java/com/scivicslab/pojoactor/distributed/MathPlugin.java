package com.scivicslab.pojoactor.distributed;

import org.json.JSONArray;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;

/**
 * Test fixture: an actor that answers by action name, so a test can check that a call made in one
 * process reaches the object in another.
 *
 * <p>Turing-workflow has a class of the same name among its own test sources, which are not
 * published, so this project carries its own rather than depending on another project's tests.
 */
public class MathPlugin implements CallableByActionName {

    private int lastResult = 0;

    public int add(int a, int b) {
        lastResult = a + b;
        return lastResult;
    }

    public int multiply(int a, int b) {
        lastResult = a * b;
        return lastResult;
    }

    public int getLastResult() {
        return lastResult;
    }

    public String greet(String name) {
        return "Hello, " + name + "!";
    }

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        try {
            return switch (actionName) {
                case "add" -> {
                    String[] p = split(args, 2);
                    yield new ActionResult(true, String.valueOf(add(Integer.parseInt(p[0]), Integer.parseInt(p[1]))));
                }
                case "multiply" -> {
                    String[] p = split(args, 2);
                    yield new ActionResult(true, String.valueOf(multiply(Integer.parseInt(p[0]), Integer.parseInt(p[1]))));
                }
                case "getLastResult" -> new ActionResult(true, String.valueOf(getLastResult()));
                case "greet" -> new ActionResult(true, greet(split(args, 1)[0]));
                default -> new ActionResult(false, "Unknown action: " + actionName);
            };
        } catch (Exception e) {
            return new ActionResult(false, actionName + " failed: " + e.getMessage());
        }
    }

    /** Accepts a JSON array or a comma-separated list, as callers of this口 have always sent both. */
    private static String[] split(String args, int expected) {
        String text = args == null ? "" : args.trim();
        String[] parts;
        if (text.startsWith("[")) {
            JSONArray a = new JSONArray(text);
            parts = new String[a.length()];
            for (int i = 0; i < a.length(); i++) parts[i] = a.get(i).toString();
        } else {
            parts = text.split(",");
        }
        if (parts.length < expected) {
            throw new IllegalArgumentException("expected " + expected + " arguments, got " + parts.length);
        }
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }
}
