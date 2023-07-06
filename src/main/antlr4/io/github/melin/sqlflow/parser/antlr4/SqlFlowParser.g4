/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

parser grammar SqlFlowParser;

options { tokenVocab=SqlFlowLexer; }

tokens {
    DELIMITER
}

parse
    : statements* EOF
    ;

statements
    : singleStatement SEMICOLON?
    |standaloneExpression SEMICOLON?
    |standalonePathSpecification SEMICOLON?
    |standaloneType SEMICOLON?
    |standaloneRowPattern SEMICOLON?
    ;

singleStatement
    : statement
    ;

standaloneExpression
    : expression
    ;

standalonePathSpecification
    : pathSpecification
    ;

standaloneType
    : type
    ;

standaloneRowPattern
    : rowPattern
    ;

statement
    : query                                                                 #statementDefault
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (query | LEFT_PAREN query RIGHT_PAREN)
        (WITH (NO)? DATA)?                                                  #createTableAsSelect
    | INSERT INTO qualifiedName partitionSpec? (IF NOT EXISTS)?
        columnAliases? query                                                #insertInto
    | INSERT OVERWRITE qualifiedName (partitionSpec (IF NOT EXISTS)?)?
        columnAliases? query                                                #insertOverWrite
    | DELETE FROM qualifiedName (WHERE booleanExpression)?                  #delete
    | CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)? qualifiedName
        (COMMENT string)?
        (WITH properties)? AS query                                         #createMaterializedView
    | CREATE (OR REPLACE)? (GLOBAL? TEMPORARY)?
        VIEW (IF NOT EXISTS)? qualifiedName
        identifierCommentList?
        (COMMENT string)?
        (SECURITY (DEFINER | INVOKER))? AS query                            #createView
    | UPDATE qualifiedName
        SET updateAssignment (COMMA updateAssignment)*
        (WHERE where=booleanExpression)?                                    #update
    | MERGE INTO qualifiedName (AS? identifier)?
        USING relation ON expression mergeCase+                             #merge
    ;

partitionSpec
    : PARTITION LEFT_PAREN partitionVal (COMMA partitionVal)* RIGHT_PAREN
    ;

partitionVal
    : identifier (EQ primaryExpression)?
    | identifier EQ DEFAULT
    ;

identifierCommentList
    : LEFT_PAREN identifierComment (COMMA identifierComment)* RIGHT_PAREN
    ;

identifierComment
    : identifier (COMMENT string)?
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

properties
    : LEFT_PAREN propertyAssignments RIGHT_PAREN
    ;

propertyAssignments
    : property (COMMA property)*
    ;

property
    : identifier EQ expression
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (COMMA sortItem)*)?
      (OFFSET offset=rowCount (ROW | ROWS)?)?
      ( (LIMIT limit=limitRowCount)
      | (FETCH (FIRST | NEXT) (fetchFirst=rowCount)? (ROW | ROWS) (ONLY | WITH TIES))
      )?
    ;

limitRowCount
    : ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | QUESTION_MARK
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                                #queryPrimaryDefault
    | TABLE qualifiedName                               #table
    | VALUES expression (COMMA expression)*             #inlineTable
    | LEFT_PAREN queryNoWith RIGHT_PAREN                #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (COMMA selectItem)*
      (FROM relation (COMMA relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (COMMA windowDefinition)*)?
    ;

groupBy
    : setQuantifier? groupingElement (COMMA groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN         #rollup
    | CUBE LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN           #cube
    | GROUPING SETS LEFT_PAREN groupingSet (COMMA groupingSet)* RIGHT_PAREN   #multipleGroupingSets
    ;

groupingSet
    : LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | expression
    ;

windowDefinition
    : name=identifier AS LEFT_PAREN windowSpecification RIGHT_PAREN
    ;

windowSpecification
    : (existingWindowName=identifier)?
      (PARTITION BY partition+=expression (COMMA partition+=expression)*)?
      (ORDER BY sortItem (COMMA sortItem)*)?
      windowFrame?
    ;

namedQuery
    : name=identifier (columnAliases)? AS LEFT_PAREN query RIGHT_PAREN
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                          #selectSingle
    | primaryExpression DOT ASTERISK (AS columnAliases)?    #selectAll
    | ASTERISK                                              #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=sampledRelation
      )                                                     #joinRelation
    | sampledRelation                                       #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING LEFT_PAREN identifier (COMMA identifier)* RIGHT_PAREN
    ;

sampledRelation
    : patternRecognition (
        TABLESAMPLE sampleType LEFT_PAREN percentage=expression RIGHT_PAREN
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

listAggOverflowBehavior
    : ERROR
    | TRUNCATE string? listaggCountIndication
    ;

listaggCountIndication
    : WITH COUNT
    | WITHOUT COUNT
    ;

patternRecognition
    : aliasedRelation (
        MATCH_RECOGNIZE LEFT_PAREN
          (PARTITION BY partition+=expression (COMMA partition+=expression)*)?
          (ORDER BY sortItem (COMMA sortItem)*)?
          (MEASURES measureDefinition (COMMA measureDefinition)*)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN LEFT_PAREN rowPattern RIGHT_PAREN withinClause?
          (SUBSET subsetDefinition (COMMA subsetDefinition)*)?
          DEFINE variableDefinition (COMMA variableDefinition)*
        RIGHT_PAREN
        (AS? identifier columnAliases?)?
      )?
    ;

measureDefinition
    : expression AS identifier
    ;

rowsPerMatch
    : ONE ROW PER MATCH
    | ALL ROWS PER MATCH emptyMatchHandling?
    ;

emptyMatchHandling
    : SHOW EMPTY MATCHES
    | OMIT EMPTY MATCHES
    | WITH UNMATCHED ROWS
    ;

skipTo
    : SKIP_ TO NEXT ROW
    | SKIP_ PAST LAST ROW
    | SKIP_ TO FIRST identifier
    | SKIP_ TO LAST identifier
    | SKIP_ TO identifier
    ;

subsetDefinition
    : name=identifier EQ LEFT_PAREN union+=identifier (COMMA union+=identifier)* RIGHT_PAREN
    ;

variableDefinition
    : identifier AS expression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : LEFT_PAREN identifier (COMMA identifier)* RIGHT_PAREN
    ;

relationPrimary
    : qualifiedName queryPeriod?                                                       #tableName
    | LEFT_PAREN query RIGHT_PAREN                                                     #subqueryRelation
    | UNNEST LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN (WITH ORDINALITY)?  #unnest
    | LATERAL LEFT_PAREN query RIGHT_PAREN                                             #lateral
    | LATERAL? TABLE LEFT_PAREN tableFunctionCall RIGHT_PAREN                          #tableFunctionInvocation
    | TABLE LEFT_PAREN windowTVFExression RIGHT_PAREN                                  #windoTVFClause
    | LEFT_PAREN relation RIGHT_PAREN                                                  #parenthesizedRelation
    ;

tableFunctionCall
    : qualifiedName LEFT_PAREN (tableFunctionArgument (COMMA tableFunctionArgument)*)?
      (COPARTITION copartitionTables (COMMA copartitionTables)*)? RIGHT_PAREN
    ;

windowTVFExression
    : windoTVFName LEFT_PAREN windowTVFParam (COMMA windowTVFParam)* RIGHT_PAREN
    ;

windoTVFName
    : TUMBLE
    | HOP
    | CUMULATE
    ;

windowTVFParam
    : TABLE qualifiedName
    | columnDescriptor
    | interval
    | DATA DOUBLE_ARROW TABLE qualifiedName
    | TIMECOL DOUBLE_ARROW columnDescriptor
    | timeIntervalParamName DOUBLE_ARROW interval
    ;

timeIntervalParamName
    : DATA
    | TIMECOL
    | SIZE
    | OFFSET
    | STEP
    | SLIDE
    ;

columnDescriptor
    : DESCRIPTOR LEFT_PAREN identifier RIGHT_PAREN
    ;

tableFunctionArgument
    : (identifier DOUBLE_ARROW)? (tableArgument | descriptorArgument | expression) // descriptor before expression to avoid parsing descriptor as a function call
    ;

tableArgument
    : tableArgumentRelation
        (PARTITION BY (LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN | expression))?
        (PRUNE WHEN EMPTY | KEEP WHEN EMPTY)?
        (ORDER BY (LEFT_PAREN sortItem (COMMA sortItem)* RIGHT_PAREN | sortItem))?
    ;

tableArgumentRelation
    : TABLE LEFT_PAREN qualifiedName RIGHT_PAREN (AS? identifier columnAliases?)?  #tableArgumentTable
    | TABLE LEFT_PAREN query RIGHT_PAREN (AS? identifier columnAliases?)?          #tableArgumentQuery
    ;

descriptorArgument
    : DESCRIPTOR LEFT_PAREN descriptorField (COMMA descriptorField)* RIGHT_PAREN
    | CAST LEFT_PAREN NULL AS DESCRIPTOR RIGHT_PAREN
    ;

descriptorField
    : identifier type?
    ;

copartitionTables
    : LEFT_PAREN qualifiedName COMMA qualifiedName (COMMA qualifiedName)* RIGHT_PAREN
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?  #predicated
    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                                                      #comparison
    | comparisonOperator comparisonQuantifier LEFT_PAREN query RIGHT_PAREN                          #quantifiedComparison
    | NOT? BETWEEN (ASYMMETRIC | SYMMETRIC)? lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN                                 #inList
    | NOT? IN LEFT_PAREN query RIGHT_PAREN                                                          #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?                            #like
    | NOT? SIMILAR TO right=valueExpression (ESCAPE escape=valueExpression)?                        #similar
    | IS NOT? NULL                                                                                  #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                                                   #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | QUESTION_MARK                                                                       #parameter
    | POSITION LEFT_PAREN valueExpression IN valueExpression RIGHT_PAREN                                 #position
    | LEFT_PAREN expression (COMMA expression)+ RIGHT_PAREN                                                #rowConstructor
    | ROW LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN                                            #rowConstructor
    | name=LISTAGG LEFT_PAREN setQuantifier? expression (COMMA string)?
        (ON OVERFLOW listAggOverflowBehavior)? RIGHT_PAREN
        (WITHIN GROUP LEFT_PAREN ORDER BY sortItem (COMMA sortItem)* RIGHT_PAREN)                          #listagg
    | processingMode? qualifiedName LEFT_PAREN (label=identifier DOT)? ASTERISK RIGHT_PAREN
        filter? over?                                                                     #functionCall
    | processingMode? qualifiedName LEFT_PAREN (setQuantifier? expression (COMMA expression)*)?
        (ORDER BY sortItem (COMMA sortItem)*)? RIGHT_PAREN filter? (nullTreatment? over)?           #functionCall
    | identifier over                                                                     #measure
    | identifier ARROW expression                                                          #lambda
    | LEFT_PAREN (identifier (COMMA identifier)*)? RIGHT_PAREN ARROW expression                             #lambda
    | LEFT_PAREN query RIGHT_PAREN                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS LEFT_PAREN query RIGHT_PAREN                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST LEFT_PAREN expression AS type RIGHT_PAREN                                                     #cast
    | TRY_CAST LEFT_PAREN expression AS type RIGHT_PAREN                                                 #cast
    | ARRAY LEFT_BRACKET (expression (COMMA expression)*)? RIGHT_BRACKET                                       #arrayConstructor
    | value=primaryExpression LEFT_BRACKET index=valueExpression RIGHT_BRACKET                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression DOT fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME (LEFT_PAREN precision=INTEGER_VALUE RIGHT_PAREN)?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP (LEFT_PAREN precision=INTEGER_VALUE RIGHT_PAREN)?                           #specialDateTimeFunction
    | name=LOCALTIME (LEFT_PAREN precision=INTEGER_VALUE RIGHT_PAREN)?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP (LEFT_PAREN precision=INTEGER_VALUE RIGHT_PAREN)?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_CATALOG                                                                #currentCatalog
    | name=CURRENT_SCHEMA                                                                 #currentSchema
    | name=CURRENT_PATH                                                                   #currentPath
    | SUBSTRING LEFT_PAREN valueExpression FROM valueExpression (FOR valueExpression)? RIGHT_PAREN       #substring
    | NORMALIZE LEFT_PAREN valueExpression (COMMA normalForm)? RIGHT_PAREN                                 #normalize
    | EXTRACT LEFT_PAREN identifier FROM valueExpression RIGHT_PAREN                                     #extract
    | LEFT_PAREN expression RIGHT_PAREN                                                                  #parenthesizedExpression
    | GROUPING LEFT_PAREN (qualifiedName (COMMA qualifiedName)*)? RIGHT_PAREN                              #groupingOperation
    ;

processingMode
    : RUNNING
    | FINAL
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | YEARS | QUARTER | MONTH | MONTHS | WEEK | WEEKS | DAY | DAYS | HOUR | HOURS | MINUTE | MINUTES | SECOND | SECONDS | MILLISECOND | MICROSECOND | NANOSECOND | EPOCH
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type
    : ROW LEFT_PAREN rowField (COMMA rowField)* RIGHT_PAREN                                       #rowType
    | INTERVAL from=intervalField (TO to=intervalField)?                                          #intervalType
    | base=TIMESTAMP (LEFT_PAREN precision = typeParameter RIGHT_PAREN)? (WITHOUT TIME ZONE)?     #dateTimeType
    | base=TIMESTAMP (LEFT_PAREN precision = typeParameter RIGHT_PAREN)? WITH TIME ZONE           #dateTimeType
    | base=TIME (LEFT_PAREN precision = typeParameter RIGHT_PAREN)? (WITHOUT TIME ZONE)?          #dateTimeType
    | base=TIME (LEFT_PAREN precision = typeParameter RIGHT_PAREN)? WITH TIME ZONE                #dateTimeType
    | DOUBLE PRECISION                                                                            #doublePrecisionType
    | ARRAY LT type GT                                                                            #legacyArrayType
    | MAP LT keyType=type COMMA valueType=type GT                                                 #legacyMapType
    | type ARRAY (LEFT_BRACKET INTEGER_VALUE RIGHT_BRACKET)?                                      #arrayType
    | identifier (LEFT_PAREN typeParameter (COMMA typeParameter)* RIGHT_PAREN)?                   #genericType
    ;

rowField
    : type
    | identifier type;

typeParameter
    : INTEGER_VALUE | type
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER LEFT_PAREN WHERE booleanExpression RIGHT_PAREN
    ;

mergeCase
    : WHEN MATCHED (AND condition=expression)? THEN
        UPDATE SET targets+=identifier EQ values+=expression
          (COMMA targets+=identifier EQ values+=expression)*                                    #mergeUpdate
    | WHEN MATCHED (AND condition=expression)? THEN DELETE                                      #mergeDelete
    | WHEN NOT MATCHED (AND condition=expression)? THEN
        INSERT (LEFT_PAREN targets+=identifier (COMMA targets+=identifier)* RIGHT_PAREN)?
        VALUES LEFT_PAREN values+=expression (COMMA values+=expression)* RIGHT_PAREN            #mergeInsert
    ;

over
    : OVER (windowName=identifier | LEFT_PAREN windowSpecification RIGHT_PAREN)
    ;

windowFrame
    : (MEASURES measureDefinition (COMMA measureDefinition)*)?
      frameExtent
      (AFTER MATCH skipTo)?
      (INITIAL | SEEK)?
      (PATTERN LEFT_PAREN rowPattern RIGHT_PAREN)?
      (SUBSET subsetDefinition (COMMA subsetDefinition)*)?
      (DEFINE variableDefinition (COMMA variableDefinition)*)?
    ;

frameExtent
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=GROUPS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    | frameType=GROUPS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

rowPattern
    : patternPrimary patternQuantifier?                 #quantifiedPrimary
    | rowPattern rowPattern                             #patternConcatenation
    | rowPattern PIPE rowPattern                        #patternAlternation
    ;

withinClause
    : WITHIN interval
    ;

patternPrimary
    : identifier                                        #patternVariable
    | LEFT_PAREN RIGHT_PAREN                                           #emptyPattern
    | PERMUTE LEFT_PAREN rowPattern (COMMA rowPattern)* RIGHT_PAREN      #patternPermutation
    | LEFT_PAREN rowPattern RIGHT_PAREN                                #groupedPattern
    | HAT                                               #partitionStartAnchor
    | DOLLAR                                               #partitionEndAnchor
    //| '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | LEFT_BRACE exactly=INTEGER_VALUE RIGHT_BRACE (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | LEFT_BRACE (atLeast=INTEGER_VALUE)? COMMA (atMost=INTEGER_VALUE)? RIGHT_BRACE (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;

updateAssignment
    : identifier EQ expression
    ;

pathElement
    : identifier DOT identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (COMMA pathElement)*
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

queryPeriod
    : FOR rangeType AS OF end=valueExpression
    ;

rangeType
    : TIMESTAMP
    | VERSION
    | SYSTEM_TIME
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

nonReserved
 //: YEAR | YEARS | QUARTER | MONTH | MONTHS | WEEK | WEEKS | DAY | DAYS | HOUR | HOURS | MINUTE | MINUTES | SECOND | SECONDS | MILLISECOND | MICROSECOND | NANOSECOND | EPOCH
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : AFTER | ALL | ANY | ARRAY | ASC | AT | ASYMMETRIC
    | BERNOULLI
    | COLUMN | COLUMNS | COMMENT | COMMIT | COUNT | CURRENT | COPARTITION | CUMULATE
    | DATA | DATE | DAY | DAYS | DEFINE | DEFINER | DESC | DESCRIPTOR | DISTRIBUTED | DOUBLE | DEFAULT
    | EMPTY | ERROR | EXCLUDING | EPOCH
    | FETCH | FILTER | FINAL | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GROUPS | GLOBAL
    | HOUR | HOURS | HOP
    | IF | IGNORE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | KEEP
    | LAST | LATERAL | LEVEL | LIMIT | LOCAL | LOGICAL
    | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASURES | MERGE | MINUTE | MINUTES | MONTH | MONTHS | MILLISECOND | MICROSECOND
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS | NANOSECOND
    | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW | OVERWRITE
    | PARTITION | PARTITIONS | PAST | PATH | PATTERN | PER | PERMUTE | POSITION | PRECEDING | PRECISION | PROPERTIES | PRUNE
    | QUARTER
    | RANGE | READ | REFRESH | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | ROW | ROWS | RUNNING
    | SECOND | SECONDS | SECURITY | SEEK | SET | SETS | SKIP_ | SIZE | STEP | SLIDE | SESSION
    | SHOW | SOME | START | STATS | SUBSET | SUBSTRING | SYSTEM | SYMMETRIC | SIMILAR
    | TABLES | TABLESAMPLE | TEXT | TIES | TIME | TIMECOL | TIMESTAMP | SYSTEM_TIME | TO | TRUNCATE | TRY_CAST | TYPE | TUMBLE | TEMPORARY
    | UNBOUNDED | UNMATCHED | UPDATE | USE | USER
    | VERSION | VIEW
    | WINDOW | WITHIN | WITHOUT | WEEK | WEEKS
    | YEAR | YEARS
    | ZONE
    ;
