package io.github.melin.sqlflow.tree;

import io.github.melin.sqlflow.AstVisitor;

import java.util.LinkedList;
import java.util.Optional;

public class StackableAstVisitor<R, C>
        extends AstVisitor<R, StackableAstVisitor.StackableAstVisitorContext<C>> {
    @Override
    public R process(Node node, StackableAstVisitorContext<C> context) {
        context.push(node);
        try {
            return node.accept(this, context);
        } finally {
            context.pop();
        }
    }

    public static class StackableAstVisitorContext<C> {
        private final LinkedList<Node> stack = new LinkedList<>();
        private final C context;

        public StackableAstVisitorContext(C context) {
            this.context = context;
        }

        public C getContext() {
            return context;
        }

        private void pop() {
            stack.pop();
        }

        void push(Node node) {
            stack.push(node);
        }

        public Optional<Node> getPreviousNode() {
            if (stack.size() > 1) {
                return Optional.of(stack.get(1));
            }
            return Optional.empty();
        }
    }
}
