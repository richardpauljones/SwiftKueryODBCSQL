/**
 Copyright IBM Corporation 2016, 2017
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import SwiftKuery
import CunixODBC
import Foundation
import Dispatch

// MARK: PostgreSQLResultFetcher

/// An implementation of query result fetcher.
public class ODBCSQLResultFetcher: ResultFetcher {
    private var titles: [String]?
    
    
    public func fetchNext(callback: @escaping (([Any?]?, Error?)) -> ()) {
        
    }
    
    public func fetchTitles(callback: @escaping (([String]?, Error?)) -> ()) {
         callback((self.titles, nil))
    }
    
    public func done() {
        
    }
    
    private static func colname(hstmt:HSTMT!,col:Int) ->String!
    {
        let bufferlength:Int=1000
        var namelength:Int16! = 0
        var datatype:Int16! = 0
        var colsize:UInt! = 0
        var decimaldigts:Int16! = 0
        var nullable:Int16! = 0
        var nullablebool:Bool = false
        
        // Todo put MAX_DATA back
        let sqlPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: 1000)
        
        var rc = CunixODBC.SQLDescribeCol(
            hstmt
            ,UInt16(col)
            ,sqlPointer
            ,SQLSMALLINT(bufferlength)
            ,&namelength
            ,&datatype
            ,&colsize
            ,&decimaldigts
            , &nullable)
        
        sqlPointer.deallocate()
        
        // Todo error handler
        /*
        if !MYSQLSUCCESS(rc)    {
            error_out(hstmt: hstmt)
            return nil
        }*/
        let sqlData = Data(bytes: sqlPointer, count: Int(namelength))
        if let data = String(data: sqlData, encoding: .utf8)    {
           return data
        }
        return nil
    }
    
    internal static func create(hstmt:HSTMT!,  callback: (ODBCSQLResultFetcher) ->()) {
        let resultFetcher = ODBCSQLResultFetcher()
        
        var columns:Int16=0
        // determine number of columns
        var rc = CunixODBC.SQLNumResultCols(hstmt,&columns)

        // todo if there is a problem need to error out
        /*
        if !MYSQLSUCCESS(rc){
            error_out(hstmt:hstmt)
            SQLFreeStmt(hstmt,SQLUSMALLINT(SQL_DROP));
            return
        }*/
        
        
        
        var columnNames = [String]()
        for column in 0 ..< columns {
            if let colname = colname(hstmt:hstmt,col:Int(column))    {
                columnNames.append(colname)
            }
        }
        resultFetcher.titles = columnNames
        
        // free up
        SQLFreeStmt(hstmt,SQLUSMALLINT(SQL_DROP));
        
        
        // toto add back
        //resultFetcher.row = resultFetcher.buildRow(queryResult: queryResult)
        callback(resultFetcher)
    }
    
    
}

