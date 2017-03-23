/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar SSMRule;

ssmrule
    : object ':' (trigger '|')? conditions '|' commands
    | Linecomment+
    ;

// TODO: Fix this item
object
    : OBJECTTYPE                            #objTypeOnly
    | OBJECTTYPE WITH boolvalue             #objTypeWith
    ;

trigger
    : AT timepointexpr                      #triTimePoint
    | EVERY timeintvalexpr duringexpr?      #triCycle
    | ON fileEvent duringexpr?              #triFileEvent
    ;

duringexpr : FROM timepointexpr (TO timepointexpr)? ;


conditions
    : boolvalue
    ;

boolvalue
    : '(' boolvalue ')'
    | compareexpr
    | NOT boolvalue
    | boolvalue AND boolvalue
    | boolvalue OR boolvalue
    | id
    | TRUE
    | FALSE
    ;

compareexpr
    : id oPCMP id                                           #cmpIdId
    | (id | LONG) oPCMP (id | LONG)                         #cmpIdLong
    | (id | STRING) ('==' | '!=') (id | STRING)             #cmpIdString
    | (id | STRING) MATCHES (id | STRING)                   #cmpIdStringMatches
    | timeintvalexpr oPCMP timeintvalexpr                   #cmpTimeintvalTimeintval
    | timepointexpr oPCMP timepointexpr                     #cmpTimepointTimePoint
    ;

timeintvalexpr
    : '(' timeintvalexpr ')'                                #tieCurves
    | TIMEINTVALCONST                                       #tieConst
    | timepointexpr '-' timepointexpr                       #tieTpExpr
    | timeintvalexpr ('-' | '+') timeintvalexpr             #tieTiExpr
    ;


timepointexpr
    : '(' timepointexpr ')'                                 #tpeCurves
    | NOW                                                   #tpeNow
    | TIMEPOINTCONST                                        #tpeTimeConst
    | timepointexpr ('+' | '-') timeintvalexpr              #tpeTimeExpr
    ;

commands
    : command (';' command)*
    ;

command
    : ID (ID | OPTION | STRING)*
    ;

commonexpr
    : '(' commonexpr ')'
    | LONG
    | STRING
    | ID
    | boolvalue
    | timeintvalexpr
    | timepointexpr
    | oprexpr
    ;

id
    : ID
    | ID '(' commonexpr (',' commonexpr)* ')'
    ;


oPCMP
    : '=='
    | '>'
    | '<'
    | '>='
    | '<='
    | '!='
    ;

opr
   : '*'
   | '/'
   | '+'
   | '-'
   | '%'
   ;

oprexpr
   : LONG opr LONG
   | STRING '+' STRING
   ;

fileEvent
   : FILECREATE
   | FILECLOSE
   | FILEAPPEND
   | FILERENAME
   | FILEMETADATA
   | FILEUNLINK
   | FILETRUNCATE
   ;


OBJECTTYPE
    : FILE
    | DIRECTORY
    | STORAGE
    | CACHE
    ;

AT : 'at' ;
AND : 'and' ;
EVERY : 'every' ;
FROM : 'from' ;
ON : 'on' ;
OR : 'or' ;
NOW : 'now' ;
NOT : 'not' ;
TO : 'to' ;
TRUE : 'true' ;
FALSE : 'false' ;
WITH : 'with' ;
MATCHES : 'matches' ;

FILE : 'file' ;
DIRECTORY : 'directory' ;
STORAGE : 'storage' ;
CACHE : 'cache' ;

FILECREATE: 'FileCreate' ;
FILECLOSE: 'FileClose' ;
FILEAPPEND: 'FileAppend' ;
FILERENAME: 'FileRename' ;
FILEMETADATA: 'FileMetadate' ;
FILEUNLINK: 'FileUnlink' ;
FILETRUNCATE: 'FileTruncate' ;


TIMEINTVALCONST
    : ([1-9] [0-9]* ('s' | 'm' | 'h' | 'd'))+ ;

TIMEPOINTCONST
    : '"' [1-9][0-9][0-9][0-9] '-' [0-9][0-9] '-' [0-9][0-9] ' '+ [0-9][0-9] ':' [0-9][0-9] ':' [0-9][0-9] '"'
    ;

ID
    : PARTID
    | PARTID '.' PARTID
    ;

fragment PARTID : [a-zA-Z_] [a-zA-Z0-9_]* ;


OPTION: '-' [a-zA-Z0-9]+ ;

Linecomment : '#' .*? '\r'? '\n' -> skip ;

WS : [ \t\r\n]+ -> skip ;

STRING
    : '"' (ESCAPE | ~["\\])* '"';
fragment ESCAPE : '\\' (["\\/bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;

LONG
    : '0'
    | [1-9] [0-9]*
    | ('0' | [1-9] [0-9]*) ('PB' | 'TB' | 'GB' | 'MB' | 'KB' | 'B')
    ;

CONST
    : LONG
    | STRING
    | TIMEINTVALCONST
    | TIMEPOINTCONST
    ;

NEWLINE : '\r'? '\n' ;
