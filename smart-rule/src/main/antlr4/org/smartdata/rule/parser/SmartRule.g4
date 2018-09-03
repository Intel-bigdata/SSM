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
grammar SmartRule;

ssmrule
    : object ':' (trigger '|')? conditions '|' cmdlet      #ruleLine
    | Linecomment+                                          #commentLine
    ;
// just one cmdlet

// TODO: Fix this item
object
    : OBJECTTYPE                            #objTypeOnly
    | OBJECTTYPE WITH objfilter             #objTypeWith
    ;

trigger
    : AT timepointexpr                      #triTimePoint
    | EVERY timeintvalexpr ('/' timeintvalexpr ('/' timeintvalexpr)? )? duringexpr?      #triCycle
    | ON fileEvent duringexpr?              #triFileEvent
    | ONCE                                  #triOnce
    ;

duringexpr : FROM timepointexpr (TO timepointexpr)? ;

objfilter
    : boolvalue
    ;

conditions
    : boolvalue
    ;

boolvalue
    : compareexpr                                           #bvCompareexpr
    | NOT boolvalue                                         #bvNot
    | boolvalue (AND | OR) boolvalue                        #bvAndOR
    | id                                                    #bvId
    | '(' boolvalue ')'                                     #bvCurve
    ;

compareexpr
    : numricexpr oPCMP numricexpr                           #cmpIdLong
    | stringexpr ('==' | '!=') stringexpr                   #cmpIdString
    | stringexpr MATCHES stringexpr                         #cmpIdStringMatches
    | timeintvalexpr oPCMP timeintvalexpr                   #cmpTimeintvalTimeintval
    | timepointexpr oPCMP timepointexpr                     #cmpTimepointTimePoint
    ;

timeintvalexpr
    : '(' timeintvalexpr ')'                                #tieCurves
    | TIMEINTVALCONST                                       #tieConst
    | timepointexpr '-' timepointexpr                       #tieTpExpr
    | timeintvalexpr ('-' | '+') timeintvalexpr             #tieTiExpr
    | id                                                    #tieTiIdExpr
    ;


timepointexpr
    : '(' timepointexpr ')'                                 #tpeCurves
    | NOW                                                   #tpeNow
    | TIMEPOINTCONST                                        #tpeTimeConst
    | timepointexpr ('+' | '-') timeintvalexpr              #tpeTimeExpr
    | id                                                    #tpeTimeId
    ;

commonexpr
    : boolvalue
    | timeintvalexpr
    | timepointexpr
    | numricexpr
    | LONG
    | STRING
    | id
    | '(' commonexpr ')'
    ;

numricexpr
   : numricexpr op=('*' | '/' | '%') numricexpr             #numricexprMul
   | numricexpr op=('+' | '-') numricexpr                   #numricexprAdd
   | id                                                     #numricexprId
   | LONG                                                   #numricexprLong
   | '(' numricexpr ')'                                     #numricexprCurve
   ;

stringexpr
   : '(' stringexpr ')'                                     #strCurve
   | STRING                                                 #strOrdString
   | TIMEPOINTCONST                                         #strTimePointStr
   | id                                                     #strID
   | stringexpr '+' stringexpr                              #strPlus
   ;

cmdlet
    : .*
    | ';'
    | '@'
    | '$'
    | '&'
    | '='
    | '{'
    | '}'
    | '['
    | ']'
    | '"'
    | '?'
    ;

id
    : ID                                                            #idAtt
    | OBJECTTYPE '.' ID                                             #idObjAtt
    | ID '(' constexpr (',' constexpr)* ')'                         #idAttPara
    | OBJECTTYPE '.' ID '(' constexpr (',' constexpr)* ')'          #idObjAttPara
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

fileEvent
   : FILECREATE
   | FILECLOSE
   | FILEAPPEND
   | FILERENAME
   | FILEMETADATA
   | FILEUNLINK
   | FILETRUNCATE
   ;

constexpr
    : LONG                                  #constLong
    | STRING                                #constString
    | TIMEINTVALCONST                       #constTimeInverval
    | TIMEPOINTCONST                        #constTimePoint
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
ONCE : 'once' ;
OR : 'or' ;
NOW : 'now' ;
NOT : 'not' ;
TO : 'to' ;
WITH : 'with' ;
MATCHES : 'matches' ;

fragment FILE : 'file' ;
fragment DIRECTORY : 'directory' ;
fragment STORAGE : 'storage' ;
fragment CACHE : 'cache' ;

FILECREATE: 'FileCreate' ;
FILECLOSE: 'FileClose' ;
FILEAPPEND: 'FileAppend' ;
FILERENAME: 'FileRename' ;
FILEMETADATA: 'FileMetadate' ;
FILEUNLINK: 'FileUnlink' ;
FILETRUNCATE: 'FileTruncate' ;


TIMEINTVALCONST
    : ([1-9] [0-9]* ('ms' | 's' | 'm' | 'h' | 'd' | 'sec' | 'min' | 'hour' | 'day'))+ ;

TIMEPOINTCONST
    : '"' [1-9][0-9][0-9][0-9] '-' [0-9][0-9] '-' [0-9][0-9] ' '+ [0-9][0-9] ':' [0-9][0-9] ':' [0-9][0-9] '"'
    ;

ID : [a-zA-Z_] [a-zA-Z0-9_]* ;

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
    | ('0' | [1-9] [0-9]*) ('P' | 'p' | 'T' | 't' | 'G' | 'g' | 'M' | 'm' | 'K' | 'k') ('B' | 'b')
    ;

NEWLINE : '\r'? '\n' ;