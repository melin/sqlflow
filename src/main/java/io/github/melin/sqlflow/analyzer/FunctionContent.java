package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.tree.NodeLocation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FunctionContent {
    private NodeLocation location;
    private String name;
    private List<String> argumentTextList;

    public FunctionContent(String name, List<String> argumentTextList) {
        this.argumentTextList = argumentTextList;
        this.name = name;
    }

    public FunctionContent(String name) {
        this.name = name;
    }

    public static FunctionContent newUnqualified(String name) {
        requireNonNull(name, "name is null");
        return new FunctionContent(name);
    }

    public static FunctionContent newUnqualified(String name, List<String> arguments) {
        requireNonNull(name, "name is null");
        return new FunctionContent(name, arguments);
    }

    public void addArgument(String argumentText) {
        if (argumentTextList == null) {
            argumentTextList = new ArrayList<>();
        }
        argumentTextList.add(argumentText);
    }

    public List<String> getArgumentTextList() {
        return argumentTextList;
    }

    public NodeLocation getLocation() {
        return location;
    }

    public void setLocation(NodeLocation location) {
        this.location = location;
    }

    public String getName() {
        return name;
    }


}
