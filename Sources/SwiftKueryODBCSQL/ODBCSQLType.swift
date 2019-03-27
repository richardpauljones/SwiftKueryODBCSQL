//
//  ODBCSQLDataType.swift
//  LoggerAPI
//
//  Created by Richard Jones on 18/03/2019.
//

import Foundation

enum ODBCSQLType: UInt32 {
    case SQL_UNKNOWN_TYPE =  0
    case SQL_CHAR =  1
    case SQL_NUMERIC =  2
    case SQL_DECIMAL =   3
    case SQL_INTEGER =  4
    case SQL_SMALLINT =  5
    case SQL_FLOAT =  6
    case SQL_REAL =  7
    case SQL_DOUBLE = 8
    case SQL_DATETIME =  9
    case SQL_VARCHAR = 12
}
