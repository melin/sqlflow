package io.github.melin.sqlflow.tree;

import io.github.melin.sqlflow.tree.join.JoinCriteria;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * huaixin 2021/12/19 11:02 AM
 */
public class NaturalJoin extends JoinCriteria {
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return (obj != null) && (getClass() == obj.getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }

    @Override
    public List<Node> getNodes() {
        return ImmutableList.of();
    }
}
