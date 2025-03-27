/**
To test in Intelij click on the parse rule and click Test parse rule. This will open a box where you can
type in an expression and view the parse result.
*/
grammar ExpressionSQLParser;

options {
    caseInsensitive = true;
}

// SQL
selectStatement: SELECT distinctOn? selectList fromClause? joinClause? whereClause? groupByClause? havingClause? orderByClause? limitClause? SEMI?;

distinctOn: DISTINCT ON OPEN_PAREN expressionList CLOSE_PAREN;

whereClause: WHERE booleanExpr (LOGIC_BINARY_BOOLEAN booleanExpr)*;
groupByClause: GROUP BY (expressionList | OPEN_PAREN expressionList CLOSE_PAREN);
havingClause: HAVING booleanExpr (LOGIC_BINARY_BOOLEAN booleanExpr)*;
orderByClause: ORDER_BY expr directions+=(ASC|DESC)? (COMMA expr directions+=(ASC|DESC)?)*;

fromClause: FROM (tableReference|selectTableReference) (COMMA (tableReference| (selectTableReference2)))*;
joinClause: joinItem+;
joinItem: directionOp=(LEFT|RIGT|FULL)? coreOp=(CROSS|INNER|OUTER)? JOIN tableReference ON booleanExpr;

limitClause: LIMIT limit=NUMBER_LITERAL+;

selectList: (exprOptionalRename|selectAllColumns) (COMMA (exprOptionalRename|selectAllColumns)?)*;
selectAllColumns: BINARY_MULTIPLICATIVE;

tableReference: exprOptionalRename #tableReferenceExpr;
selectTableReference: OPEN_PAREN selectStatement CLOSE_PAREN AS? newName=IDENTIFIER;
selectTableReference2: (lateral=LATERAL)? selectTableReference;

notExpr: NOT expr;
// from table as
exprOptionalRename: expr (AS? newName=(IDENTIFIER|FUNCTION_NAMES))?;

/*
We need expressions to be recursively defineable.
Antlr4 only supports direct left recursive rules, which means
all our expressions that recurse on expr need to be part of the expr rule
*/
expr:
    caseWhenExpr #caseWhenExpr2
    | NOT expr #notExpr2
    | DISTINCT expr #distinctExpr
    | expr BINARY_RIGHT_MULTIPLCATIVE<assoc=right> expr #rightMultiplicativeBinaryExpr //done
    | left=expr JSON_GET (STRING_LITERAL|NUMBER_LITERAL) #jsonGetExpr // as jsonGet function call
    | OPEN_PAREN expr CLOSE_PAREN #parenExpr
    | left=expr TYPECAST type=TYPE #typeCastExpr // as typeCast function call
    | left=expr BINARY_MULTIPLICATIVE right=expr #multiplicateBinaryExpr  //done
    | left=expr BINARY_ADDITIVE right=expr #additiveBinaryExpr  //done
    | left=expr ARITHMETIC_BINARY_BOOLEAN right=expr #arithmeticBinaryBooleanExpr2  //done
    | left=expr OTHER_BINARY_BOOLEAN right=expr #otherBinaryBooleanExpr2  //done
    | left=expr LOGIC_BINARY_BOOLEAN right=expr #logicalBinaryBooleanExpr2  //done
    | expr SUFFIX_UNARY_BOOLEAN #suffixUnaryBooleanExpr2 //done
    | FUNCTION_NAMES OPEN_PAREN (DISTINCT expr | BINARY_MULTIPLICATIVE|expressionList)? CLOSE_PAREN #funcCallExpr //done
    | columnSelection #columnSelection2 //done exitColumnSelection2
    | sql_datum #sqlDatum //done exitStringDatum, exitBooleanDatum, exitNumberDatum, exitNullDatum,
    ;

expressionList: expr (COMMA expr)*;
columnSelection:
    tableOrColumn=IDENTIFIER (DOT column=IDENTIFIER)+; // Handles column selection with dot notation

sql_datum:
      STRING_LITERAL #stringDatum
      | NUMBER_LITERAL #numberDatum
      | (TRUE|FALSE) #booleanDatum
      | NULL  #nullDatum
      | IDENTIFIER # identifierDatum;


whenExprCondition: (expr|booleanExpr) (condition=LOGIC_BINARY_BOOLEAN (booleanExpr))?;
whenExpr: WHEN whenExprCondition THEN result=expr;
caseWhenExpr: CASE whenExpr+ (ELSE elseResult=expr)? END;

/*
Infix operators
*/

/*
Suffix operators
*/

booleanExpr: otherBinaryBooleanExpr | logicalBooleanExpr | unaryBooleanExpr | aritheticBooleanExpr | caseWhenExpr;

otherBinaryBooleanExpr: left=expr OTHER_BINARY_BOOLEAN right=expr;

unaryBooleanExpr: NOT OPEN_PAREN expr CLOSE_PAREN #unaryBooleanExprtParens
        | expr IS NULL #unaryBooleanExprIsNull
        | expr IS NOT NULL #unaryBooleanExprIsNotNull;

aritheticBooleanExpr: left=expr ARITHMETIC_BINARY_BOOLEAN right=expr;
logicalBooleanExpr: left=expr LOGIC_BINARY_BOOLEAN right=expr;


/*
 Vocabulary
*/

FUNCTION_NAMES: 'date_trunc'
    | 'regexp_split_to_array'
    | 'regexp_split_to_table'
    | 'json_array_length'
    | 'json_each'
    | 'json_each_text'
    | 'json_object_keys'
    | 'json_array_elements'
    | 'json_strip_nulls'
    | 'json_agg'
    | 'json_build_object'
    | 'now'
    | 'to_timestamp'
    | 'to_date'
    | 'sum'
    | 'count'
    | 'max'
    | 'min'
    | 'avg'
    | 'greatest'
    | 'least'
    | 'concat'
    | 'string_agg'
    | 'replace'
    | 'rtrim'
    | 'ltrim'
    | 'substring'
    | 'trim'
    | 'upper'
    | 'lower'
    | 'regexp_replace'
    | 'length'
    | 'split_part'
    | 'position'
    | 'md5'
    | 'regexp_matches'
    | 'format'
    | 'concat_ws'
    | 'left'
    | 'right'
    | 'reverse'
    | 'char_length'
    | 'to_hex'
    | 'btrim'
    | 'grok'
    | 'abs'
    | 'ceil'
    | 'floor'
    | 'round'
    | 'sqrt'
    | 'power'
    | 'mod'
    | 'random'
    | 'trunc'
    | 'sign'
    | 'gcd'
    | 'lcm'
    | 'exp'
    | 'log'
    | 'ln'
    | 'log10'
    | 'log2'
    | 'degrees'
    | 'radians'
    | 'pi'
    | 'sin'
    | 'cos'
    | 'tan';

NUMBER_LITERAL: MINUS? [_0-9]+ ('.' [_0-9]*)?;

NOT: 'NOT';
LATERAL: 'LATERAL';

OPEN_PAREN: '(';
CLOSE_PAREN: ')';

ARITHMETIC_BINARY_BOOLEAN: EQUAL | NOT_EQUALS | LT | GT | LTE | GTE;
OTHER_BINARY_BOOLEAN: LIKE | IN | ILIKE;
LOGIC_BINARY_BOOLEAN: AND | OR;
PREFIX_UNARY_BOOLEAN: NOT;
SUFFIX_UNARY_BOOLEAN: IS NULL | IS NOT NULL;

JSON_GET: ('->' | '->>');

TYPE: 'INT' | 'TEXT' | 'DATE' | 'TIME' | 'TIMESTAMP' | 'JSON' | 'NUMERIC' | 'DOUBLE PRECISION' |'BOOLEAN' | 'BOOL';
TYPE_ARRAY: TYPE OPEN_BRACKET CLOSE_BRACKET;

ORDER_BY: 'ORDER BY';
ASC: 'ASC';
DESC: 'DESC';
DISTINCT: 'DISTINCT';

CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
BETWEEN: 'BETWEEN';

SELECT: 'SELECT';
WHERE: 'WHERE';
FROM: 'FROM';
GROUP: 'GROUP';
BY: 'BY';
HAVING: 'HAVING';
LIMIT: 'LIMIT';

SINGLE_LINE_COMMENT: '--' ~[\r\n]* (('\r'? '\n') | EOF) -> channel(HIDDEN);
MULTILINE_COMMENT: '/*' .*? '*/' -> channel(HIDDEN);

OPEN_BRACKET: '[';
CLOSE_BRACKET: ']';
COMMA: ',';
SEMI: ';';
COLON: ':';
EQUAL: '=';
LT: '<';
GT: '>';
LTE: '<=';
GTE: '>=';
NOT_EQUALS: '<>' | '!=';
LIKE: 'LIKE';
ILIKE: 'ILIKE';

BINARY_MULTIPLICATIVE: PERCENT | MULTIPLY | DIVIDE;
BINARY_RIGHT_MULTIPLCATIVE: CARET;
BINARY_ADDITIVE: MINUS | PLUS | STRING_CONCAT;


MULTIPLY: '*';
PLUS: '+';
MINUS: '-';
DIVIDE: '/';
CARET: '^';
PERCENT: '%';
OVERLAP_OP: '&&';

ANY: 'ANY';
ALL: 'ALL';

STRING_CONCAT: '||';
AS: 'AS';
//CAST: 'CAST';
//INTERVAL: 'INTERVAL';
IN: 'IN';
NULL: 'NULL';

AND: 'AND';
OR: 'OR';
DOT: '.';
IS: 'IS';
LEFT: 'LEFT';
RIGT: 'RIGHT';
OUTER: 'OUTER';
INNER: 'INNER';
FULL: 'FULL';
CROSS: 'CROSS';
JOIN: 'JOIN';
ON: 'ON';

TYPECAST: '::';

TRUE: 'TRUE';
FALSE: 'FALSE';


IDENTIFIER: IDENTIFIER_1 | IDENTIFIER_2;
IDENTIFIER_1: [a-z_] [a-z0-9_]*;
IDENTIFIER_2: '"' (ESC | .)*? '"';
fragment ESC: '\\' '"';

STRING_LITERAL: '\'' (~'\'' | TRIPLE_SINGLE_QUOTE)+ '\'' || '\'\'';
TRIPLE_SINGLE_QUOTE: '\'' '\'' '\''; // Three consecutive single quotes represent a single quote within a string


WS : [ \t\r\n]+ -> skip;

NON_RECOGNISED_INPUT: .;
