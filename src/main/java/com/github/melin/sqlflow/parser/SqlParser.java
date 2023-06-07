package com.github.melin.sqlflow.parser;

import com.github.melin.sqlflow.autogen.SqlFlowStatementBaseListener;
import com.github.melin.sqlflow.autogen.SqlFlowStatementLexer;
import com.github.melin.sqlflow.autogen.SqlFlowStatementParser;
import com.github.melin.sqlflow.tree.Node;
import com.github.melin.sqlflow.tree.statement.Statement;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 11:04 PM
 */
public class SqlParser {
    private static final BiConsumer<SqlFlowStatementLexer, SqlFlowStatementParser> DEFAULT_PARSER_INITIALIZER =
            (SqlFlowStatementLexer lexer, SqlFlowStatementParser parser) -> {
    };

    private final BiConsumer<SqlFlowStatementLexer, SqlFlowStatementParser> initializer;

    public SqlParser() {
        this(DEFAULT_PARSER_INITIALIZER);
    }

    public SqlParser(BiConsumer<SqlFlowStatementLexer, SqlFlowStatementParser> initializer) {
        this.initializer = requireNonNull(initializer, "initializer is null");
    }

    public Statement createStatement(String sql) {
        try {
            return (Statement) invokeParser("statement", sql, SqlFlowStatementParser::singleStatement, new ParsingOptions());
        } catch (ParseException e) {
            if(StringUtils.isNotBlank(e.getCommand())) {
                throw e;
            } else {
                throw e.withCommand(sql);
            }
        }
    }

    private Node invokeParser(String name, String sql,
                              Function<SqlFlowStatementParser, ParserRuleContext> parseFunction,
                              ParsingOptions parsingOptions) {
        try {
            UpperCaseCharStream charStream = new UpperCaseCharStream(CharStreams.fromString(sql));
            SqlFlowStatementLexer lexer = new SqlFlowStatementLexer(charStream);
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            SqlFlowStatementParser parser = new SqlFlowStatementParser(tokenStream);
            initializer.accept(lexer, parser);

            // Override the default error strategy to not attempt inserting or deleting a token.
            // Otherwise, it messes up error reporting
            parser.setErrorHandler(new DefaultErrorStrategy() {
                @Override
                public Token recoverInline(Parser recognizer)
                        throws RecognitionException {
                    if (nextTokensContext == null) {
                        throw new InputMismatchException(recognizer);
                    } else {
                        throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                    }
                }
            });

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

            lexer.removeErrorListeners();
            lexer.addErrorListener(new ParseErrorListener());

            parser.removeErrorListeners();
            parser.addErrorListener(new ParseErrorListener());

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            } catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            }

            return new AstBuilder(parsingOptions).visit(tree);
        } catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    private static class PostProcessor
            extends SqlFlowStatementBaseListener {
        private final List<String> ruleNames;
        private final SqlFlowStatementParser parser;

        public PostProcessor(List<String> ruleNames, SqlFlowStatementParser parser) {
            this.ruleNames = ruleNames;
            this.parser = parser;
        }

        @Override
        public void exitQuotedIdentifier(SqlFlowStatementParser.QuotedIdentifierContext context) {
            Token token = context.QUOTED_IDENTIFIER().getSymbol();
            if (token.getText().length() == 2) { // empty identifier
                throw new ParsingException("Zero-length delimited identifier not allowed", null, token.getLine(), token.getCharPositionInLine() + 1);
            }
        }

        @Override
        public void exitBackQuotedIdentifier(SqlFlowStatementParser.BackQuotedIdentifierContext context) {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "backquoted identifiers are not supported; use double quotes to quote identifiers",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine() + 1);
        }

        @Override
        public void exitDigitIdentifier(SqlFlowStatementParser.DigitIdentifierContext context) {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; surround the identifier with double quotes",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine() + 1);
        }

        @Override
        public void exitNonReserved(SqlFlowStatementParser.NonReservedContext context) {
            // we can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
            // Otherwise, ANTLR gets confused and fires spurious notifications.
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved words with IDENT tokens
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            Token newToken = new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    SqlFlowStatementLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex());

            context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
        }
    }
}
