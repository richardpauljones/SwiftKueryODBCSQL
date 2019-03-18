//
//  String_extension.swift
//  LoggerAPI
//
//  Created by Richard Jones on 16/03/2019.
//

import Foundation

extension String {
    
    public func stringToUnsafeMutablePointer() -> UnsafeMutablePointer<UInt8> {
        
        let data = self.data(using: String.Encoding.utf8, allowLossyConversion: false)!
        let dataMutablePointer = UnsafeMutablePointer<UInt8>.allocate(capacity: data.count)
        
        //Copies the bytes to the Mutable Pointer
        data.copyBytes(to: dataMutablePointer, count: data.count)
        
        
        return dataMutablePointer
    }
    public var lengthint16:Int16 {
        get { return  Int16(self.count)
            
        }
        
    }
    
}
