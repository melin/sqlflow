package io.github.melin.sqlflow.tree;

import java.util.Optional;

/**
 * huaixin 2021/12/18 11:16 PM
 */
public abstract class SelectItem
        extends Node {
    protected SelectItem(Optional<NodeLocation> location) {
        super(location);
    }
}
