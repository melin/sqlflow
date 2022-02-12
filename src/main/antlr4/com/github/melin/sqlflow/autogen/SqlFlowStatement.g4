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

grammar SqlFlowStatement;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standalonePathSpecification
    : pathSpecification EOF
    ;

standaloneType
    : type EOF
    ;

standaloneRowPattern
    : rowPattern EOF
    ;

statement
    : query                                                            #statementDefault
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (query | '('query')')
        (WITH (NO)? DATA)?                                             #createTableAsSelect
    | INSERT INTO qualifiedName columnAliases? query                   #insertInto
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)? qualifiedName
        (COMMENT string)?
        (WITH properties)? AS query                                    #createMaterializedView
    | CREATE (OR REPLACE)? VIEW qualifiedName
        (COMMENT string)?
        (SECURITY (DEFINER | INVOKER))? AS query                       #createView
    | UPDATE qualifiedName
        SET updateAssignment (',' updateAssignment)*
        (WHERE where=booleanExpression)?                               #update
    | MERGE INTO qualifiedName (AS? identifier)?
        USING relation ON expression mergeCase+                        #merge
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

properties
    : '(' propertyAssignments ')'
    ;

propertyAssignments
    : property (',' property)*
    ;

property
    : identifier EQ expression
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
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
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith ')'                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (',' windowDefinition)*)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP '(' (expression (',' expression)*)? ')'         #rollup
    | CUBE '(' (expression (',' expression)*)? ')'           #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   #multipleGroupingSets
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

windowDefinition
    : name=identifier AS '(' windowSpecification ')'
    ;

windowSpecification
    : (existingWindowName=identifier)?
      (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                          #selectSingle
    | primaryExpression '.' ASTERISK (AS columnAliases)?    #selectAll
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
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : patternRecognition (
        TABLESAMPLE sampleType '(' percentage=expression ')'
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
        MATCH_RECOGNIZE '('
          (PARTITION BY partition+=expression (',' partition+=expression)*)?
          (ORDER BY sortItem (',' sortItem)*)?
          (MEASURES measureDefinition (',' measureDefinition)*)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN '(' rowPattern ')'
          (SUBSET subsetDefinition (',' subsetDefinition)*)?
          DEFINE variableDefinition (',' variableDefinition)*
        ')'
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
    : 'SKIP' TO NEXT ROW
    | 'SKIP' PAST LAST ROW
    | 'SKIP' TO FIRST identifier
    | 'SKIP' TO LAST identifier
    | 'SKIP' TO identifier
    ;

subsetDefinition
    : name=identifier EQ '(' union+=identifier (',' union+=identifier)* ')'
    ;

variableDefinition
    : identifier AS expression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName queryPeriod?                                      #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
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
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
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
    | POSITION '(' valueExpression IN valueExpression ')'                                 #position
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | name=LISTAGG '(' setQuantifier? expression (',' string)?
        (ON OVERFLOW listAggOverflowBehavior)? ')'
        (WITHIN GROUP '(' ORDER BY sortItem (',' sortItem)* ')')                          #listagg
    | processingMode? qualifiedName '(' (label=identifier '.')? ASTERISK ')'
        filter? over?                                                                     #functionCall
    | processingMode? qualifiedName '(' (setQuantifier? expression (',' expression)*)?
        (ORDER BY sortItem (',' sortItem)*)? ')' filter? (nullTreatment? over)?           #functionCall
    | identifier over                                                                     #measure
    | identifier '->' expression                                                          #lambda
    | '(' (identifier (',' identifier)*)? ')' '->' expression                             #lambda
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | TRY_CAST '(' expression AS type ')'                                                 #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?                           #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_CATALOG                                                                #currentCatalog
    | name=CURRENT_SCHEMA                                                                 #currentSchema
    | name=CURRENT_PATH                                                                   #currentPath
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
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
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type
    : ROW '(' rowField (',' rowField)* ')'                                         #rowType
    | INTERVAL from=intervalField (TO to=intervalField)?                           #intervalType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?     #dateTimeType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? WITH TIME ZONE           #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?          #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? WITH TIME ZONE                #dateTimeType
    | DOUBLE PRECISION                                                             #doublePrecisionType
    | ARRAY '<' type '>'                                                           #legacyArrayType
    | MAP '<' keyType=type ',' valueType=type '>'                                  #legacyMapType
    | type ARRAY ('[' INTEGER_VALUE ']')?                                          #arrayType
    | identifier ('(' typeParameter (',' typeParameter)* ')')?                     #genericType
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
    : FILTER '(' WHERE booleanExpression ')'
    ;

mergeCase
    : WHEN MATCHED (AND condition=expression)? THEN
        UPDATE SET targets+=identifier EQ values+=expression
          (',' targets+=identifier EQ values+=expression)*                  #mergeUpdate
    | WHEN MATCHED (AND condition=expression)? THEN DELETE                  #mergeDelete
    | WHEN NOT MATCHED (AND condition=expression)? THEN
        INSERT ('(' targets+=identifier (',' targets+=identifier)* ')')?
        VALUES '(' values+=expression (',' values+=expression)* ')'         #mergeInsert
    ;

over
    : OVER (windowName=identifier | '(' windowSpecification ')')
    ;

windowFrame
    : (MEASURES measureDefinition (',' measureDefinition)*)?
      frameExtent
      (AFTER MATCH skipTo)?
      (INITIAL | SEEK)?
      (PATTERN '(' rowPattern ')')?
      (SUBSET subsetDefinition (',' subsetDefinition)*)?
      (DEFINE variableDefinition (',' variableDefinition)*)?
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
    | rowPattern '|' rowPattern                         #patternAlternation
    ;

patternPrimary
    : identifier                                        #patternVariable
    | '(' ')'                                           #emptyPattern
    | PERMUTE '(' rowPattern (',' rowPattern)* ')'      #patternPermutation
    | '(' rowPattern ')'                                #groupedPattern
    | '^'                                               #partitionStartAnchor
    | '$'                                               #partitionEndAnchor
    | '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | '{' exactly=INTEGER_VALUE '}' (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | '{' (atLeast=INTEGER_VALUE)? ',' (atMost=INTEGER_VALUE)? '}' (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;

updateAssignment
    : identifier EQ expression
    ;

pathElement
    : identifier '.' identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (',' pathElement)*
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

queryPeriod
    : FOR rangeType AS OF end=valueExpression
    ;

rangeType
    : TIMESTAMP
    | VERSION
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
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : AFTER | ALL | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | COLUMN | COLUMNS | COMMENT | COMMIT | COUNT | CURRENT
    | DATA | DATE | DAY | DEFINE | DEFINER | DESC | DISTRIBUTED | DOUBLE
    | EMPTY | ERROR | EXCLUDING
    | FETCH | FILTER | FINAL | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GROUPS
    | HOUR
    | IF | IGNORE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LAST | LATERAL | LEVEL | LIMIT | LOCAL | LOGICAL
    | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASURES | MERGE | MINUTE | MONTH
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PAST | PATH | PATTERN | PER | PERMUTE | POSITION | PRECEDING | PRECISION | PROPERTIES
    | RANGE | READ | REFRESH | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | ROW | ROWS | RUNNING
    | SECOND | SECURITY | SEEK | SET | SETS
    | SHOW | SOME | START | STATS | SUBSET | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIES | TIME | TIMESTAMP | TO | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNMATCHED | UPDATE | USE | USER
    | VERSION | VIEW
    | WINDOW | WITHIN | WITHOUT
    | YEAR
    | ZONE
    ;

AFTER: 'AFTER';
ALL: 'ALL';
ALTER: 'ALTER';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BY: 'BY';
CASE: 'CASE';
CAST: 'CAST';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
CONSTRAINT: 'CONSTRAINT';
COUNT: 'COUNT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_CATALOG: 'CURRENT_CATALOG';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_PATH: 'CURRENT_PATH';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DEFINE: 'DEFINE';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
END: 'END';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FETCH: 'FETCH';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
GROUPS: 'GROUPS';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INITIAL: 'INITIAL';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JOIN: 'JOIN';
JSON: 'JSON';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LISTAGG: 'LISTAGG';
LOCAL: 'LOCAL';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MEASURES: 'MEASURES';
MERGE: 'MERGE';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NATURAL: 'NATURAL';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OFFSET: 'OFFSET';
OMIT: 'OMIT';
OF: 'OF';
ON: 'ON';
ONE: 'ONE';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
OVERFLOW: 'OVERFLOW';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PAST: 'PAST';
PATH: 'PATH';
PATTERN: 'PATTERN';
PER: 'PER';
PERMUTE: 'PERMUTE';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PROPERTIES: 'PROPERTIES';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
REFRESH: 'REFRESH';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RIGHT: 'RIGHT';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
RUNNING: 'RUNNING';
SECOND: 'SECOND';
SECURITY: 'SECURITY';
SEEK: 'SEEK';
SELECT: 'SELECT';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNION: 'UNION';
UNMATCHED: 'UNMATCHED';
UNNEST: 'UNNEST';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALUES: 'VALUES';
VERSION: 'VERSION';
VIEW: 'VIEW';
WHEN: 'WHEN';
WHERE: 'WHERE';
WINDOW: 'WINDOW';
WITH: 'WITH';
WITHIN: 'WITHIN';
WITHOUT: 'WITHOUT';
YEAR: 'YEAR';
ZONE: 'ZONE';

EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
QUESTION_MARK: '?';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    : 'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
