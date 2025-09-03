grammar EvalExpression;

language
    : expression EOF
    ;

expression
    : conditionalOr
    ;

conditionalOr
    : conditionalAnd ( 'or' conditionalAnd )*
    ;

conditionalAnd
    : equality ( 'and' equality )*
    ;

equality
    : relational ( ( '==' | '!=' ) relational )*
    ;

relational
    : additive ( ( '<' | '>' | '<=' | '>=' ) additive )*
    ;

additive
    : multiplicative ( ( '+' | '-' ) multiplicative )*
    ;

multiplicative
    : unary ( ( '*' | '/' | '%' ) unary )*
    ;

unary
    : ( '-' | 'not' )? primary
    ;

primary
    : (value
    | genericFunctionCall
    | dictExpression
    | caseExpression
    | IDENTIFIER
    | pathSelectExpr
    | '(' expression ')') (step)*
    ;

genericFunctionCall
    : IDENTIFIER '(' (arguments)? ')'
    ;

arguments
    : expression (',' expression)*
    ;

/*
case expression syntax:
```
case(
    condition_1 => expression_1,
    condition_2 => expression_2,
    ...
    condition_n => expression_n,
    _ => default_expression
)
```
The condition check happens from condition_1 sequentially to condition_n.
On the first condition that evalutats to `true`, the corresponding expression will be evaluated and the result returned.
In this case, the rest of the conditions are skipped.
If no condition is evaluated to `true`, the default expression is executed and returned
*/
caseExpression
    : 'case' '(' whenClause (',' whenClause)* ',' elseClause ')'
    ;

whenClause
    : expression '=>' expression
    ;

elseClause
    :  '_' '=>' expression
    ;

/*
dictFunction:
Construct a diction (map) of String to object.
Example:
```
dict(
  city=$.address.city,
  state=$.address.state,
  foo= dict(
    tag0=$.tags[0],
    tag1=$.tags[1]
  )
)
```
The left hand side of the assignemnt (kvPair) is the key and the right hand side is the value expression.
After evaluating the right hand side expression, its value is set as the value to the corresponding key.
*/
dictExpression
    : 'dict' '(' (kvPair (',' kvPair)*)? ')'
    ;

kvPair
    : dictKey '=' expression
    ;

dictKey
    : IDENTIFIER(('.'|'::')IDENTIFIER)*
    ;

/*
pathSelectExpr:
Point to a field in an event.

- use `.` notation to select a key in a map/dict
- use `[<number>]` to select an element in an array.
- use `$` to point to the root event.

For example:
For the given input event:
```
{
    "f1": {
        "f1_1": "abc"
    },
    "f2":[1, 2]
}
```
- `$` points to the entire input event.
- `$.f1.f1_1` points to `"abc"`.
- `$.f2[0]` points to `1`.
*/
pathSelectExpr
    : '$' (step)*
    ;

step
    : fieldAccess | arrayAccess
    ;

fieldAccess
    : '[' QUOTED_IDENTIFIER ']' | '.' IDENTIFIER
    ;

arrayAccess
    : '[' expression ']'
    ;

value
    : NUMBER_LITERAL
    | INT_LITERAL
    | QUOTED_IDENTIFIER
    | BOOLEAN_LITERAL
    | NULL_LITERAL
    ;


// Lexer Tokens (unchanged)
INT_LITERAL
    : '0' | [1-9][0-9]*
    ;

NUMBER_LITERAL
    : INT_LITERAL ('.' [0-9]+)? | '.' [0-9]+
    ;

BOOLEAN_LITERAL
    : ('true' | 'false')
    ;

NULL_LITERAL
    : 'null'
    ;

IDENTIFIER
    : [a-zA-Z_] [a-zA-Z0-9_]*
    ;

QUOTED_IDENTIFIER
    : '"' (~["\\]+ | '\\' .)* '"' | '\'' (~['\\]+ | '\\' .)* '\''
    ;

WHITESPACE
    : [ \t\r\n]+ -> skip
    ;