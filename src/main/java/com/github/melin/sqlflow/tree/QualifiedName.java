package com.github.melin.sqlflow.tree;

import com.github.melin.sqlflow.tree.expression.Identifier;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 10:02 PM
 */
public class QualifiedName {
    private final List<String> parts;
    private final List<Identifier> originalParts;

    public static QualifiedName of(String first, String... rest) {
        requireNonNull(first, "first is null");
        return of(ImmutableList.copyOf(Lists.asList(first, rest).stream().map(Identifier::new).collect(Collectors.toList())));
    }

    public static QualifiedName of(String name) {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(new Identifier(name)));
    }

    public static QualifiedName of(Iterable<Identifier> originalParts) {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");

        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    private QualifiedName(List<Identifier> originalParts) {
        this.originalParts = originalParts;
        this.parts = originalParts.stream().map(identifier -> identifier.getValue().toLowerCase(ENGLISH)).collect(toImmutableList());
    }

    public List<String> getParts() {
        return parts;
    }

    public List<Identifier> getOriginalParts() {
        return originalParts;
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix() {
        if (parts.size() == 1) {
            return Optional.empty();
        }

        List<Identifier> subList = originalParts.subList(0, originalParts.size() - 1);
        return Optional.of(new QualifiedName(subList));
    }

    public boolean hasSuffix(QualifiedName suffix) {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();

        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    public String getSuffix() {
        return Iterables.getLast(parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode() {
        return parts.hashCode();
    }
}
