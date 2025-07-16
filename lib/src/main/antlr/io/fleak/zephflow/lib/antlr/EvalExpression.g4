grammar EvalExpression;

/* Tokens */
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

fragment
ESC
    : '\\"'
    ;
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
    | function
    | IDENTIFIER
    | pathSelectExpr
    | '(' expression ')'
    | caseExpression) (step)*
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

value
    : NUMBER_LITERAL
    | INT_LITERAL
    | QUOTED_IDENTIFIER
    | BOOLEAN_LITERAL
    | NULL_LITERAL
    ;

function
    : dictFunction
    | dictMergeFunction
    | arrayFunction
    | arrForEachFunction
    | arrFlattenFunction
    | strSplitFunction
    | tsStrToEpochFunction
    | epochToTsStrFunction
    | durationStrToMillsFunction
    | parseIntFunction
    | parseFloatFunction
    | grokFunction
    | strUpperFunction
    | strLowerFunction
    | toStringFunction
    | strContainsFunction
    | sizeFunction
    | pythonFunction
    | rangeFunction
    ;

/*
rangeFunction:
Generates an array of integer numbers based on the provided arguments.
The 'end' parameter in all variations is exclusive (the range goes up to,
but does not include, 'end').

Syntax and Behavior:

1. range(count)
   - Generates integers from 0 up to (but not including) 'count', with a step of 1.
   - Equivalent to range(0, count, 1).
   - Example: range(5) produces [0, 1, 2, 3, 4].
   - If 'count' is 0 or negative, an empty array is generated.
   - Example: range(-2) produces [].

2. range(start, end)
   - Generates integers from 'start' up to (but not including) 'end', with a step of 1.
   - Equivalent to range(start, end, 1).
   - Example: range(2, 5) produces [2, 3, 4].
   - If 'start' is greater than or equal to 'end', an empty array is generated.
   - Example: range(5, 2) produces [].

3. range(start, end, step)
   - Generates integers starting from 'start', incrementing by 'step', and stopping
     before reaching 'end'.
   - 'step' can be positive (to count up) or negative (to count down).
   - 'step' cannot be zero.
   - If 'step' is positive: numbers 'x' are generated as long as 'x < end'.
     Example: range(0, 10, 2) produces [0, 2, 4, 6, 8].
   - If 'step' is negative: numbers 'x' are generated as long as 'x > end'.
     Example: range(10, 0, -2) produces [10, 8, 6, 4, 2].
   - An empty array is generated if the conditions for generation are not met
     from the start (e.g., if 'start >= end' with a positive 'step', or
     if 'start <= end' with a negative 'step').
     Example: range(0, 5, -1) produces [].
     Example: range(5, 0, 1) produces [].
*/
rangeFunction
    : 'range' '(' rangeArgs ')'
    ;

rangeArgs
    : expression (',' expression)? (',' expression)? // For range(count), range(start, end), or range(start, end, step)
    ;

/*
pythonFunction:
Execute a single Python function automatically discovered within the script string.

Syntax:
python("<python_script_with_one_function_def>", arg1, arg2, ...)

- The first argument MUST be a string literal containing the Python script.
  This script MUST define exactly one top-level function usable as the entry point.
- Subsequent arguments (arg1, arg2, ...) are standard FEEL expressions whose
  evaluated values will be passed to the discovered Python function.
- Returns the value returned by the Python function, converted back to FEEL data types.
- Throws an error if zero or more than one function is found in the script.
- Requires GraalVM with Python language support configured.
*/
pythonFunction
    : 'python' '(' QUOTED_IDENTIFIER (',' expression)* ')' // Script + optional args
    ;

/*
sizeFunction:
return the size of the argument. Supported input argument types
- array: return number of elements in the array
- object/dict/map: return number of key-value pairs
- string: return the string size
*/
sizeFunction
    : 'size_of' '(' expression ')'
    ;

/*
strContainsFunction:
test if the given string contains a substring.

Syntax:
str_contains(str, sub_str)

return `true` if `str` contains `sub_str`, otherwise `false`.
both arguments are required to be of string type
*/
strContainsFunction
    : 'str_contains' '(' strContainsArg ')'
    ;

strContainsArg
    : expression ',' expression
    ;

/*
toStringFunction:
convert the input argument to String.
If argument is null, return null (not `"null"`)
*/
toStringFunction
    : 'to_str' '(' expression ')'
    ;

/*
strUpperFunction:
convert string to upper case. argument must be a string
*/
strUpperFunction
    : 'upper' '(' expression ')'
    ;

/*
strLowerFunction:
convert string to lower case. argument must be a string
*/
strLowerFunction
    : 'lower' '(' expression ')'
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
dictFunction
    : 'dict' '(' (dictArg)? ')'
    ;
dictArg
    : kvPair (',' kvPair)*
    ;

/*
dictMergeFunction:
Merge a set of dictionaries into one.
For example:
```
dict_merge(
  $,
  grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
)
```
(`$` refers to the current input event)

It's required that all arguments are dictionaries.
For every argument, add all its key value pairs to the result dictionary. Duplicated keys are overriden.

returns a merged dict
*/
dictMergeFunction
    : 'dict_merge' '('dictMergeArg')'
    ;
dictMergeArg
    : expression (',' expression)*
    ;

/*
arrayFunction:
Evaluate each argument and combine the result values into an array.
For example:
Given an input event:
```
{
    "f1": "a",
    "f2": "b"
}
```
The function call:
```
array($.f1, $.f2)
```
Results: `["a", "b"]`
*/
arrayFunction
    : 'array' '(' (arrayArg)? ')'
    ;
arrayArg
    : expression (',' expression)*
    ;

/*
arrForEachFunction:
Apply the same logic for every element in a given array.
Returns a new array containing the transformed elements.
Syntax:
```
arr_foreach($.path.to.array, variable_name, expression)
```
where
- `$.path.to.array` points to the array field in the input event.
- If `$.path.to.array` points to an object, treat that object as a single element array
- declares a variable name that represents each array element
- the expression logic to be applied on every array element. use `variable_name` to refer to the array element

For example:
Given the input event:
```
{
  "integration": "snmp",
  "attachments": {
    "snmp_pdf": "s3://a.pdf",
    "f1": "s3://b.pdf"
  },
  "resp": {
    "Test1": [
      {
        "operation_system": "windows",
        "ipAddr": "1.2.3.4"
      },
      {
        "operation_system": "windows",
        "ipAddr": "1.2.3.5"
      }
    ]
  }
}

```

and the `arr_foreach` expression:
```
arr_foreach(
    $.resp.Test1,
    elem,
    dict(
        osVersion=elem.operation_system,
        source=$.integration,
        pdf_attachment=$.attachments.snmp_pdf,
        ip=elem.ipAddr
    )
)
```

Results:
```
[
    {
      "source": "snmp",
      "osVersion": "windows",
      "pdf_attachment": "s3://a.pdf",
      "ip": "1.2.3.4"
    },
    {
      "source": "snmp",
      "osVersion": "windows",
      "pdf_attachment": "s3://a.pdf",
      "ip": "1.2.3.5"
    }
]
```
*/
arrForEachFunction
    : 'arr_foreach' '(' arrForEachArg ')'
    ;
arrForEachArg
    : expression ',' IDENTIFIER ',' expression
    ;

/*
arr_flatten:
Transforms an array containing nested arrays into an array by extracting all
immediate elements from the first level of nesting only. This function performs a shallow
flatten operation, not a deep recursive flatten.

Syntax:
```
arr_flatten(arr_of_arr)
```

Parameters:
- arr_of_arr: An array that may contain both simple elements and nested arrays at its first level.

Return Value:
- A new array with only one level of nesting removed. Deeper nested arrays remain intact.

Behavior:
- Extracts elements from first-level nested arrays only
- Does not recursively flatten deeper nested arrays
- Preserves the original order of elements
- Non-array elements are included as-is in the result

For example, given the input event:
```
{
  "f": [
    [1, [2, 3]],
    [4, 5],
    6
  ]
}
```

When applying:
```
arr_flatten($.f)
```

Results in:
```
[1, [2, 3], 4, 5, 6]
```
Note that the inner array [2, 3] remains intact as the function only flattens one level.
*/
arrFlattenFunction
    : 'arr_flatten' '(' arrFlattenArg ')'
    ;
arrFlattenArg
    : expression
    ;

/*
strSplitFunction:
Split a string into an array of substrings based on a delimiter.
Syntax:
```
str_split(string, delimiter)
```
*/
strSplitFunction
    : 'str_split' '(' strSplitArg ')'
    ;
strSplitArg
    : expression ',' expression
    ;

/*
tsStrToEpochFunction:
Convert a datetime string input epoch milliseconds.
Syntax:
```
ts_str_to_epoch($.path.to.timestamp.field, "<date_time_pattern>")
```
where
- `$.path.to.timestamp.field` points to the field that contains the timestamp string value
- `<date_time_pattern>` is a string literal that represents Unicode Date Format Patterns
The implementation uses java `SimpleDateFormat` to parse the timestamp string
*/
tsStrToEpochFunction
    : 'ts_str_to_epoch' '(' tsStrToEpochArg ')'
    ;
tsStrToEpochArg
    : expression ',' QUOTED_IDENTIFIER
    ;

/*
epochToTsStrFunction:
Convert an epoch millisecond timestamp into a human readable string.
Syntax:
```
epoch_to_ts_str($.path.to.timestamp.field, "<date_time_pattern>")
```
where
- $.path.to.timestamp.field points to the field that contains the epoch millis timestamp
- "<date_time_pattern>" is a string literal that represents Unicode Date Format Patterns
The implementation uses java `SimpleDateFormat` to stringify the epoch millis timestamp
*/
epochToTsStrFunction
    : 'epoch_to_ts_str' '(' epochToTsStrArg ')'
    ;
epochToTsStrArg
    : expression ',' QUOTED_IDENTIFIER
    ;

/*
durationStrToMillsFunction:
Convert a duration string in the format of `HH:mm:ss` to milliseconds

For example:
```
duration_str_to_mills("0:01:07")
```
returns `67000`

*/
durationStrToMillsFunction
    : 'duration_str_to_mills' '(' expression ')'
    ;

/*
parseIntFunction:
Parse a string into an integer. It's equivalent to Java `Long.parseLong()`

For example:
```
parse_int("3")
```
returns `3`.
*/
parseIntFunction
    : 'parse_int' '('parseIntArg')'
    ;
parseIntArg
    : expression (',' INT_LITERAL)?
    ;

/*
parseFloatFunction:
Parse a string into an float number. It's equivalent to Java `Double.parseDouble()`

For example:
```
parse_float("3.14")
```
returns `3.14`.
*/
parseFloatFunction
    : 'parse_float' '('parseFloatArg')'
    ;
parseFloatArg
    : expression
    ;



/*
grokFunction:
Apply grok pattern to a given string field and return a dictionary with all grok extracted fields

Syntax:
grok($.path.to.string.field, "<grok_pattern>")

For example:
Given the following input event:
```
{
  "__raw__": "Oct 10 2018 12:34:56 localhost CiscoASA[999]: %ASA-6-305011: Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
}
```

And the grok function:
```
grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
```

Results:
```
{
    "timestamp": "Oct 10 2018 12:34:56",
    "hostname": "localhost",
    "program": "CiscoASA",
    "pid": "999",
    "level": "6",
    "message_number": "305011",
    "message_text": "Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
}
```
*/
grokFunction
    : 'grok' '(' grokArg ')'
    ;

grokArg
    : expression ',' QUOTED_IDENTIFIER
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
    : '$'(step)*
    ;

step
    : fieldAccess | arrayAccess
    ;

fieldAccess
    : '[' QUOTED_IDENTIFIER ']' |'.' IDENTIFIER
    ;

arrayAccess
    : '[' expression ']'
    ;