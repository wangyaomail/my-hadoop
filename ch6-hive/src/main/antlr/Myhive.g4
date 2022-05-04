grammar Myhive;

INT     : [0-9]+;
WS      : [ \t\n]+ -> skip;
NAME    : [a-zA-Z_]+;

PLUS    : '+';

select_stmt_1
    : 'select * from students' EOF
    ;

select_stmt_2
    : 'select' col_name 'from' NAME EOF
    ;

col_name
    : NAME
    ;