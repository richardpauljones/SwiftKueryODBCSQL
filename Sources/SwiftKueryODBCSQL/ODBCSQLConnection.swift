//
//  ODBCSQLConnection.swift
//  
//
//  Created by Richard Jones on 14/03/2019.
//

// inspired by
// https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/connecting-with-sqlconnect?view=sql-server-2017

import SwiftKuery
import Dispatch
import Foundation
import CunixODBC

enum ConnectionState {
    case idle, runningQuery, fetchingResultSet
}

// MARK: ODBCSQLConnection

public class ODBCSQLConnection: Connection {
    var connection: UUID?
    
    private var connectionParameters: String = ""
    private var dsn:String = ""
    private var username:String = ""
    private var password:String = ""
    
    private var preparedStatements = Set<String>()
    
    // MARK:Execute Handling

    public func execute(preparedStatement: PreparedStatement, parameters: [String : Any?], onCompletion: @escaping ((QueryResult) -> ())) {
          print("OK 1")
    }
    
    public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
          print("OK 2")
    }
    
    public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
          print("OK 3")
    }
    
    
    public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        print("OK 4")
    }
    
    private func buildQuery(_ query: Query) throws  -> String {
        // NOTE: The following call into SwiftKuery does not pack binary types
        // properly - it still uses String(describing:) instead of yielding a Data object.
        var Query = try query.build(queryBuilder: queryBuilder)
        if let insertQuery = query as? Insert, insertQuery.returnID {
            let columns = insertQuery.table.columns.filter { $0.isPrimaryKey && $0.autoIncrement }
            
            if (insertQuery.suffix == nil && columns.count == 1) {
                let insertQueryReturnID = insertQuery.suffix("Returning " + columns[0].name)
                Query = try insertQueryReturnID.build(queryBuilder: queryBuilder)
            }
            
            if (insertQuery.suffix != nil) {
                throw QueryError.syntaxError("Suffix for query already set, could not add Returning suffix")
            }
        }
        return Query
    }
    
    
    /// Execute a raw query.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: raw, preparedStatement: nil, with: [Any?](), onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: raw, preparedStatement: nil, with: parameters, onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        runCompletionHandler(.error(QueryError.unsupported("TODO")), onCompletion: onCompletion)
    }

    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let odbcquery = try self.buildQuery(query)
            return execute(query: odbcquery, preparedStatement: nil, with: [Any?](), onCompletion: onCompletion)
        } catch QueryError.syntaxError(let error) {
            return runCompletionHandler(.error(QueryError.syntaxError(error)), onCompletion: onCompletion)
        } catch {
            return runCompletionHandler(.error(QueryError.syntaxError("Failed to build the query")), onCompletion: onCompletion)
        }
    }
    
    
    private func execute(query: String?, preparedStatement: PreparedStatement?, with parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
            guard connection != nil else {
                return self.runCompletionHandler(.error(QueryError.connection("Connection is disconnected")), onCompletion: onCompletion)
            }
        
            DispatchQueue.global().async {
            
                var torun = ""
        
                if let _query = query    {
                    torun = _query
                }
        
                if (torun == "")    {
                    return self.runCompletionHandler(.error(QueryError.connection("Nothing To Run")), onCompletion: onCompletion)
                }
                print(torun)
            
                var hstmt:HSTMT!
                var rc = SQLAllocStmt(self.hdbc, &hstmt)
                if let error = self.odbcerror(rc,hstmt:hstmt) {
                    SQLFreeStmt(hstmt,SQLUSMALLINT(SQL_DROP));
                    return self.runCompletionHandler(.error(QueryError.databaseError(error)), onCompletion: onCompletion)
                }
                rc = SQLExecDirect(hstmt, torun.stringToUnsafeMutablePointer(), SQLINTEGER(torun.count));
        
                if let error = self.odbcerror(rc,hstmt:hstmt) {
                    SQLFreeStmt(hstmt,SQLUSMALLINT(SQL_DROP));
                    return self.runCompletionHandler(.error(QueryError.databaseError(error)), onCompletion: onCompletion)
                }
                SQLFreeStmt(hstmt,SQLUSMALLINT(SQL_DROP));
                
                self.processQueryResult(query: torun, onCompletion: onCompletion)
                //return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
        }
    }
    
    
    private func processQueryResult(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        /*guard let result = PQgetResult(connection) else {
            setState(.idle)
            var errorMessage = "No result returned for query: \(query)."
            if let error = String(validatingUTF8: PQerrorMessage(connection)) {
                errorMessage += " Error: \(error)."
            }
            runCompletionHandler(.error(QueryError.noResult(errorMessage)), onCompletion: onCompletion)
            return
        }*/

       runCompletionHandler(.successNoData, onCompletion: onCompletion)

        /*let status = PQresultStatus(result)
        if status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK {
            // Since we set the single row mode, PGRES_TUPLES_OK means the result is empty, i.e. there are
            // no rows to return.
            clearResult(result, connection: self)
            runCompletionHandler(.successNoData, onCompletion: onCompletion)
        }
        else if status == PGRES_SINGLE_TUPLE {
            PostgreSQLResultFetcher.create(queryResult: result, connection: self) { resultFetcher in
                self.setState(.fetchingResultSet)
                self.currentResultFetcher = resultFetcher
                runCompletionHandler(.resultSet(ResultSet(resultFetcher, connection: self)), onCompletion: onCompletion)
            }
        }
        else {
            let errorMessage = String(validatingUTF8: PQresultErrorMessage(result)) ?? "Unknown"
            clearResult(result, connection: self)
            runCompletionHandler(.error(QueryError.databaseError("Query execution error:\n" + errorMessage + " For query: " + query)), onCompletion: onCompletion)
        }*/
    }

    
    
    
    
    // MARK:Initilisation
    
    /// Init with a set of connectionparameters
    ///
    /// - Parameter connectionParameters: Takes the form
    /// Driver=ODBC Driver 17 for SQL Server; DATABASE=<DATABASE>; Server=<SERVER>; UID=<USERNAME>; PWD=<PASSWORD>
    public init(connectionParameters: String) {
        self.connectionParameters = connectionParameters
        queryBuilder = ODBCSQLConnection.createQueryBuilder()
    }
    
    /// Init with a set of connectionparameters
    ///
    /// - Parameter dsn: Data Source Name
    /// - Parameter username: a username
    /// - Parameter password: a password
    public init(dsn: String, username:String = "", password:String = "") {
        self.dsn = dsn
        self.username = username
        self.password = password
        
        queryBuilder = ODBCSQLConnection.createQueryBuilder()
    }
    
    // MARK:Pools

    /// Create a connection pool.
    ///
    /// - Parameter connectionParameters: Takes the form
    /// Driver=ODBC Driver 17 for SQL Server; DATABASE=<DATABASE>; Server=<SERVER>; UID=<USERNAME>; PWD=<PASSWORD>
    /// - Returns: The `ConnectionPool` of `OSDBCSQLConnection`.
    public static func createPool(connectionParameters: String, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        //let connectionParameters = extractConnectionParameters(host: host, port: port, options: options)
        return createPool( connectionParameters, options: poolOptions)
    }
    
    private static func createPool(_ connectionParameters: String, options: ConnectionPoolOptions) -> ConnectionPool {
        let connectionGenerator: () -> Connection? = {
            let connection = ODBCSQLConnection(connectionParameters: connectionParameters)
            let ret = connection.connectSync()
            if let err = ret.asError
            {
                print(err.localizedDescription)
                // TODO need to improve
                return nil
            }
            print("Connected Through Pool")
            return connection
            
            }
        let connectionReleaser: (_ connection: Connection) -> () = { connection in
            connection.closeConnection()
        }
        
        return ConnectionPool(options: options, connectionGenerator: connectionGenerator, connectionReleaser: connectionReleaser)
        }
    
    // TODO Not touched yet
    private static func createQueryBuilder() -> QueryBuilder {
        let queryBuilder = QueryBuilder(withDeleteRequiresUsing: true, withUpdateRequiresFrom: true, columnBuilder: ODBCSQLColumnBuilder())
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
                                          QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
                                          QueryBuilder.QuerySubstitutionNames.len : "LENGTH",
                                          QueryBuilder.QuerySubstitutionNames.numberedParameter : "$",
                                          QueryBuilder.QuerySubstitutionNames.namedParameter : "",
                                          QueryBuilder.QuerySubstitutionNames.double : "double precision",
                                          QueryBuilder.QuerySubstitutionNames.uuid : "uuid"
            ])
        return queryBuilder
    }
    
    public var queryBuilder: QueryBuilder
    
    // MARK:Connections
    public func connect(onCompletion: @escaping (QueryResult) -> ()) {
        
        if self.connectionParameters == ""  && self.dsn == "" {
            return self.runCompletionHandler(.error(QueryError.connection("No connection parameters.")), onCompletion: onCompletion)
        }
        DispatchQueue.global().async {
            self.odbcinitalise()
            
            
            let sqlPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: self.MAX_DATA)
            var rc:SQLRETURN = 0
            if self.dsn != ""    {  // Connect through a DSN
                rc = CunixODBC.SQLConnectA(self.hdbc
                    , self.dsn.stringToUnsafeMutablePointer()
                    , SQLSMALLINT(SQL_NTS)
                    , self.username.stringToUnsafeMutablePointer()
                    , self.username.lengthint16
                    , self.password.stringToUnsafeMutablePointer()
                    , self.password.lengthint16)
            }   else    {
                var connouta:Int16 = 0
                rc = CunixODBC.SQLDriverConnect(self.hdbc
                    ,nil
                    , self.connectionParameters.stringToUnsafeMutablePointer()
                    , self.connectionParameters.lengthint16
                    , sqlPointer
                    ,SQLSMALLINT(self.MAX_DATA),&connouta,0)
            }
            sqlPointer.deallocate()
            
            if let error = self.odbcerror(rc)  {
                return self.runCompletionHandler(.error(QueryError.connection(error)), onCompletion: onCompletion)
            }
            self.connection = UUID.init()
            
            return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
        }
    }
    
    public func connectSync() -> QueryResult {
        var result: QueryResult? = nil
        let semaphore = DispatchSemaphore(value: 0)
        connect() { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return .error(QueryError.connection("ConnectSync unexpetedly return a nil QueryResult"))
        }
        return resultUnwrapped
    }
    
    public func closeConnection() {
            SQLDisconnect(hdbc)
            SQLFreeConnect(hdbc)
            SQLFreeEnv(henv)
            connection = nil
    
        print("Disconnected")
    }
    
    public var isConnected: Bool
    {
        get
        {
            return connection != nil
            
        }
    }
    
    // MARK:Prepares

    public func prepareStatement(_ query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func prepareStatement(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func descriptionOf(query: Query) throws -> String {
        return "OK"
    }
    
    // MARK:Transactions

    public func startTransaction(onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        
    }

    // MARK:ODBC Lowl Level

    private var henv:SQLHENV! // Environment
    private var hdbc:SQLHDBC! // Connection Handle
   // private var szData:String! // Returned data storage
   // private var cbData:Int! = 0 // Output length of data

    private let MAX_DATA = 100
  
    private func odbcerror(_ rc:SQLRETURN,hstmt:HSTMT! = nil)-> String!
    {
        if rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO { return nil }
       
        var errorout="";
        var nativeerror:Int32 = 0
        var sqlstate:UInt8 = 0
        
        let messagetext = UnsafeMutablePointer<UInt8>.allocate(capacity: Int(SQL_MAX_MESSAGE_LENGTH) + 1)
        defer { messagetext.deallocate() }
        
        var messagelength:SQLSMALLINT! = 0
        while SQLError(henv,hdbc,hstmt,&sqlstate,&nativeerror, messagetext,SQLSMALLINT(SQL_MAX_MESSAGE_LENGTH),&messagelength) == SQL_SUCCESS
        {
            var line="Error: "
            line += "State=\(sqlstate) "
            line += "Native Error=\(nativeerror) "
            
            let sqlmsg = Data(bytes: messagetext, count: Int(messagelength))
            if let data = String(data: sqlmsg, encoding: .utf8)    {
                line += "\(data)"
            }
            if errorout.count > 0 { errorout += "\r\n" }
            errorout += line
        }
    print(errorout)
        return errorout
    }
    
    private func odbcinitalise()
    {
        CunixODBC.SQLAllocEnv(&henv)
        CunixODBC.SQLAllocHandle(CunixODBC.SQLSMALLINT(CunixODBC.SQL_HANDLE_DBC), henv, &hdbc)
    }
}

// Todo class has not been touched yet
class ODBCSQLColumnBuilder: ColumnCreator {
    func buildColumn(for column: Column, using queryBuilder: QueryBuilder) -> String? {
        guard let type = column.type else {
            return nil
        }
        
        var result = column.name
        let identifierQuoteCharacter = queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.identifierQuoteCharacter.rawValue]
        if !result.hasPrefix(identifierQuoteCharacter) {
            result = identifierQuoteCharacter + result + identifierQuoteCharacter + " "
        }
        
        var typeString = type.create(queryBuilder: queryBuilder)
        if let length = column.length {
            typeString += "(\(length))"
        }
        if column.autoIncrement {
            guard let autoIncrementType = getAutoIncrementType(for: typeString) else {
                //Unrecognised type for autoIncrement column, return nil
                return nil
            }
            result += autoIncrementType
        } else {
            result += typeString
        }
        
        if column.isPrimaryKey {
            result += " PRIMARY KEY"
        }
        if column.isNotNullable {
            result += " NOT NULL"
        }
        if column.isUnique {
            result += " UNIQUE"
        }
        if let defaultValue = column.defaultValue {
            var packedType: String
            do {
                packedType = try packType(defaultValue, queryBuilder: queryBuilder)
            } catch {
                return nil
            }
            result += " DEFAULT " + packedType
        }
        if let checkExpression = column.checkExpression {
            result += checkExpression.contains(column.name) ? " CHECK (" + checkExpression.replacingOccurrences(of: column.name, with: "\"\(column.name)\"") + ")" : " CHECK (" + checkExpression + ")"
        }
        if let collate = column.collate {
            result += " COLLATE \"" + collate + "\""
        }
        return result
    }
    
    func getAutoIncrementType(for type: String) -> String? {
        switch type {
        case "smallint":
            return "smallserial "
        case "integer":
            return "serial "
        case "bigint":
            return "bigserial "
        default:
            return nil
        }
    }
    
    func packType(_ item: Any, queryBuilder: QueryBuilder) throws -> String {
        switch item {
        case let val as String:
            return "'\(val)'"
        case let val as Bool:
            return val ? queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.booleanTrue.rawValue]
                : queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.booleanFalse.rawValue]
        case let val as Parameter:
            return try val.build(queryBuilder: queryBuilder)
        case let value as Date:
            if let dateFormatter = queryBuilder.dateFormatter {
                return dateFormatter.string(from: value)
            }
            return "'\(String(describing: value))'"
        default:
            return String(describing: item)
        }
    }
}
