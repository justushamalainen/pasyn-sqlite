//! Protocol for Unix socket communication between writer server and clients
//!
//! This module defines the message format for serializing write requests
//! and responses over a Unix socket.

use std::io::{self, Read, Write};

use crate::value::Value;

/// Magic bytes to identify our protocol
const PROTOCOL_MAGIC: &[u8; 4] = b"PSQL";

/// Protocol version (2 = with request_id support)
const PROTOCOL_VERSION: u8 = 2;

/// Request types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RequestType {
    /// Execute a SQL statement (no results expected)
    Execute = 1,
    /// Execute a SQL statement and return last insert rowid
    ExecuteReturningRowId = 2,
    /// Execute multiple statements (batch)
    ExecuteBatch = 3,
    /// Begin a transaction
    BeginTransaction = 4,
    /// Commit the current transaction
    Commit = 5,
    /// Rollback the current transaction
    Rollback = 6,
    /// Ping to check if server is alive
    Ping = 7,
    /// Shutdown the server
    Shutdown = 8,
    /// Execute same SQL with multiple parameter sets (executemany)
    ExecuteMany = 9,
    /// Acquire exclusive transaction lock (returns token)
    AcquireTransactionLock = 10,
    /// Release exclusive transaction lock
    ReleaseTransactionLock = 11,
}

impl TryFrom<u8> for RequestType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RequestType::Execute),
            2 => Ok(RequestType::ExecuteReturningRowId),
            3 => Ok(RequestType::ExecuteBatch),
            4 => Ok(RequestType::BeginTransaction),
            5 => Ok(RequestType::Commit),
            6 => Ok(RequestType::Rollback),
            7 => Ok(RequestType::Ping),
            8 => Ok(RequestType::Shutdown),
            9 => Ok(RequestType::ExecuteMany),
            10 => Ok(RequestType::AcquireTransactionLock),
            11 => Ok(RequestType::ReleaseTransactionLock),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown request type: {}", value),
            )),
        }
    }
}

/// Response status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResponseStatus {
    /// Operation succeeded
    Ok = 0,
    /// Operation failed with error
    Error = 1,
}

impl TryFrom<u8> for ResponseStatus {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, io::Error> {
        match value {
            0 => Ok(ResponseStatus::Ok),
            1 => Ok(ResponseStatus::Error),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown response status: {}", value),
            )),
        }
    }
}

/// A request to the writer server
#[derive(Debug, Clone)]
pub struct Request {
    /// Unique request ID for multiplexing (echoed back in response)
    pub request_id: u64,
    pub request_type: RequestType,
    pub sql: Option<String>,
    pub params: Vec<Value>,
    /// Multiple parameter sets for ExecuteMany
    pub params_batch: Vec<Vec<Value>>,
    /// Transaction token for exclusive lock operations
    pub transaction_token: Option<u64>,
}

impl Request {
    /// Create a new execute request
    pub fn execute(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::Execute,
            sql: Some(sql.into()),
            params,
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create an execute request that returns the last insert rowid
    pub fn execute_returning_rowid(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::ExecuteReturningRowId,
            sql: Some(sql.into()),
            params,
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create an execute_many request (same SQL, multiple parameter sets)
    pub fn execute_many(sql: impl Into<String>, params_batch: Vec<Vec<Value>>) -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::ExecuteMany,
            sql: Some(sql.into()),
            params: Vec::new(),
            params_batch,
            transaction_token: None,
        }
    }

    /// Create a batch execute request
    pub fn execute_batch(sql: impl Into<String>) -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::ExecuteBatch,
            sql: Some(sql.into()),
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a begin transaction request
    pub fn begin_transaction() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::BeginTransaction,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a commit request
    pub fn commit() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::Commit,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a rollback request
    pub fn rollback() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::Rollback,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a ping request
    pub fn ping() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::Ping,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a shutdown request
    pub fn shutdown() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::Shutdown,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a request to acquire exclusive transaction lock
    pub fn acquire_transaction_lock() -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::AcquireTransactionLock,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: None,
        }
    }

    /// Create a request to release exclusive transaction lock
    pub fn release_transaction_lock(token: u64) -> Self {
        Request {
            request_id: 0,
            request_type: RequestType::ReleaseTransactionLock,
            sql: None,
            params: Vec::new(),
            params_batch: Vec::new(),
            transaction_token: Some(token),
        }
    }

    /// Set the request ID
    pub fn with_id(mut self, id: u64) -> Self {
        self.request_id = id;
        self
    }

    /// Set the transaction token
    pub fn with_token(mut self, token: u64) -> Self {
        self.transaction_token = Some(token);
        self
    }

    /// Serialize the request to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic + version
        buf.extend_from_slice(PROTOCOL_MAGIC);
        buf.push(PROTOCOL_VERSION);

        // Request ID (8 bytes, little endian)
        buf.extend_from_slice(&self.request_id.to_le_bytes());

        // Request type
        buf.push(self.request_type as u8);

        // SQL (length-prefixed)
        if let Some(ref sql) = self.sql {
            let sql_bytes = sql.as_bytes();
            buf.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(sql_bytes);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Params count
        buf.extend_from_slice(&(self.params.len() as u32).to_le_bytes());

        // Serialize each param
        for param in &self.params {
            serialize_value(&mut buf, param);
        }

        // Params batch count (for ExecuteMany)
        buf.extend_from_slice(&(self.params_batch.len() as u32).to_le_bytes());

        // Serialize each params set in batch
        for params in &self.params_batch {
            buf.extend_from_slice(&(params.len() as u32).to_le_bytes());
            for param in params {
                serialize_value(&mut buf, param);
            }
        }

        // Transaction token (1 byte flag + optional 8 bytes)
        match self.transaction_token {
            Some(token) => {
                buf.push(1); // has token
                buf.extend_from_slice(&token.to_le_bytes());
            }
            None => {
                buf.push(0); // no token
            }
        }

        buf
    }

    /// Deserialize a request from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        let mut cursor = io::Cursor::new(data);
        Self::read_from(&mut cursor)
    }

    /// Read a request from a stream
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != PROTOCOL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid protocol magic",
            ));
        }

        // Version
        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != PROTOCOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported protocol version: {}", version[0]),
            ));
        }

        // Request ID
        let mut id_bytes = [0u8; 8];
        reader.read_exact(&mut id_bytes)?;
        let request_id = u64::from_le_bytes(id_bytes);

        // Request type
        let mut req_type = [0u8; 1];
        reader.read_exact(&mut req_type)?;
        let request_type = RequestType::try_from(req_type[0])?;

        // SQL length
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let sql_len = u32::from_le_bytes(len_bytes) as usize;

        // SQL
        let sql = if sql_len > 0 {
            let mut sql_bytes = vec![0u8; sql_len];
            reader.read_exact(&mut sql_bytes)?;
            Some(String::from_utf8(sql_bytes).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UTF-8: {}", e))
            })?)
        } else {
            None
        };

        // Params count
        reader.read_exact(&mut len_bytes)?;
        let params_count = u32::from_le_bytes(len_bytes) as usize;

        // Params
        let mut params = Vec::with_capacity(params_count);
        for _ in 0..params_count {
            params.push(deserialize_value(reader)?);
        }

        // Params batch count
        reader.read_exact(&mut len_bytes)?;
        let batch_count = u32::from_le_bytes(len_bytes) as usize;

        // Params batch
        let mut params_batch = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            reader.read_exact(&mut len_bytes)?;
            let set_count = u32::from_le_bytes(len_bytes) as usize;
            let mut param_set = Vec::with_capacity(set_count);
            for _ in 0..set_count {
                param_set.push(deserialize_value(reader)?);
            }
            params_batch.push(param_set);
        }

        // Transaction token (1 byte flag + optional 8 bytes)
        // For backwards compatibility, treat EOF as no token
        let transaction_token = {
            let mut has_token = [0u8; 1];
            match reader.read_exact(&mut has_token) {
                Ok(()) if has_token[0] == 1 => {
                    let mut token_bytes = [0u8; 8];
                    reader.read_exact(&mut token_bytes)?;
                    Some(u64::from_le_bytes(token_bytes))
                }
                Ok(()) => None, // has_token[0] == 0
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
                Err(e) => return Err(e),
            }
        };

        Ok(Request {
            request_id,
            request_type,
            sql,
            params,
            params_batch,
            transaction_token,
        })
    }
}

/// A response from the writer server
#[derive(Debug, Clone)]
pub struct Response {
    /// Request ID (echoed back from request for multiplexing)
    pub request_id: u64,
    pub status: ResponseStatus,
    pub rows_affected: i64,
    pub last_insert_rowid: i64,
    pub error_message: Option<String>,
    /// Transaction token (returned when lock is acquired)
    pub transaction_token: Option<u64>,
}

impl Response {
    /// Create a success response
    pub fn ok(rows_affected: i64, last_insert_rowid: i64) -> Self {
        Response {
            request_id: 0,
            status: ResponseStatus::Ok,
            rows_affected,
            last_insert_rowid,
            error_message: None,
            transaction_token: None,
        }
    }

    /// Create an error response
    pub fn error(message: impl Into<String>) -> Self {
        Response {
            request_id: 0,
            status: ResponseStatus::Error,
            rows_affected: 0,
            last_insert_rowid: 0,
            error_message: Some(message.into()),
            transaction_token: None,
        }
    }

    /// Create a simple OK response (for ping, commit, etc.)
    pub fn simple_ok() -> Self {
        Self::ok(0, 0)
    }

    /// Create a success response with a transaction token
    pub fn ok_with_token(token: u64) -> Self {
        Response {
            request_id: 0,
            status: ResponseStatus::Ok,
            rows_affected: 0,
            last_insert_rowid: 0,
            error_message: None,
            transaction_token: Some(token),
        }
    }

    /// Set the request ID (echoed from request)
    pub fn with_id(mut self, id: u64) -> Self {
        self.request_id = id;
        self
    }

    /// Set the transaction token
    pub fn with_token(mut self, token: u64) -> Self {
        self.transaction_token = Some(token);
        self
    }

    /// Serialize the response to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic + version
        buf.extend_from_slice(PROTOCOL_MAGIC);
        buf.push(PROTOCOL_VERSION);

        // Request ID (8 bytes, little endian)
        buf.extend_from_slice(&self.request_id.to_le_bytes());

        // Status
        buf.push(self.status as u8);

        // Rows affected
        buf.extend_from_slice(&self.rows_affected.to_le_bytes());

        // Last insert rowid
        buf.extend_from_slice(&self.last_insert_rowid.to_le_bytes());

        // Error message
        if let Some(ref msg) = self.error_message {
            let msg_bytes = msg.as_bytes();
            buf.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(msg_bytes);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Transaction token (1 byte flag + optional 8 bytes)
        match self.transaction_token {
            Some(token) => {
                buf.push(1); // has token
                buf.extend_from_slice(&token.to_le_bytes());
            }
            None => {
                buf.push(0); // no token
            }
        }

        buf
    }

    /// Deserialize a response from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        let mut cursor = io::Cursor::new(data);
        Self::read_from(&mut cursor)
    }

    /// Read a response from a stream
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != PROTOCOL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid protocol magic",
            ));
        }

        // Version
        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != PROTOCOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported protocol version: {}", version[0]),
            ));
        }

        // Request ID
        let mut id_bytes = [0u8; 8];
        reader.read_exact(&mut id_bytes)?;
        let request_id = u64::from_le_bytes(id_bytes);

        // Status
        let mut status_byte = [0u8; 1];
        reader.read_exact(&mut status_byte)?;
        let status = ResponseStatus::try_from(status_byte[0])?;

        // Rows affected
        let mut i64_bytes = [0u8; 8];
        reader.read_exact(&mut i64_bytes)?;
        let rows_affected = i64::from_le_bytes(i64_bytes);

        // Last insert rowid
        reader.read_exact(&mut i64_bytes)?;
        let last_insert_rowid = i64::from_le_bytes(i64_bytes);

        // Error message length
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let msg_len = u32::from_le_bytes(len_bytes) as usize;

        // Error message
        let error_message = if msg_len > 0 {
            let mut msg_bytes = vec![0u8; msg_len];
            reader.read_exact(&mut msg_bytes)?;
            Some(String::from_utf8(msg_bytes).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UTF-8: {}", e))
            })?)
        } else {
            None
        };

        // Transaction token (1 byte flag + optional 8 bytes)
        // For backwards compatibility, treat EOF as no token
        let transaction_token = {
            let mut has_token = [0u8; 1];
            match reader.read_exact(&mut has_token) {
                Ok(()) if has_token[0] == 1 => {
                    let mut token_bytes = [0u8; 8];
                    reader.read_exact(&mut token_bytes)?;
                    Some(u64::from_le_bytes(token_bytes))
                }
                Ok(()) => None, // has_token[0] == 0
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
                Err(e) => return Err(e),
            }
        };

        Ok(Response {
            request_id,
            status,
            rows_affected,
            last_insert_rowid,
            error_message,
            transaction_token,
        })
    }

    /// Check if this response is successful
    pub fn is_ok(&self) -> bool {
        self.status == ResponseStatus::Ok
    }

    /// Convert to a Result
    pub fn into_result(self) -> Result<(i64, i64), String> {
        if self.is_ok() {
            Ok((self.rows_affected, self.last_insert_rowid))
        } else {
            Err(self
                .error_message
                .unwrap_or_else(|| "Unknown error".to_string()))
        }
    }
}

/// Value type tags for serialization
#[repr(u8)]
enum ValueTag {
    Null = 0,
    Integer = 1,
    Real = 2,
    Text = 3,
    Blob = 4,
}

fn serialize_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => {
            buf.push(ValueTag::Null as u8);
        }
        Value::Integer(i) => {
            buf.push(ValueTag::Integer as u8);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Real(f) => {
            buf.push(ValueTag::Real as u8);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(ValueTag::Text as u8);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Blob(b) => {
            buf.push(ValueTag::Blob as u8);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

fn deserialize_value<R: Read>(reader: &mut R) -> io::Result<Value> {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;

    match tag[0] {
        0 => Ok(Value::Null),
        1 => {
            let mut bytes = [0u8; 8];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Integer(i64::from_le_bytes(bytes)))
        }
        2 => {
            let mut bytes = [0u8; 8];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Real(f64::from_le_bytes(bytes)))
        }
        3 => {
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)?;
            let s = String::from_utf8(bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Value::Text(s))
        }
        4 => {
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Blob(bytes))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown value type: {}", tag[0]),
        )),
    }
}

/// Write a length-prefixed message to a stream
pub fn write_message<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    // Write length (4 bytes, little endian)
    writer.write_all(&(data.len() as u32).to_le_bytes())?;
    // Write data
    writer.write_all(data)?;
    writer.flush()
}

/// Read a length-prefixed message from a stream
pub fn read_message<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    // Read length
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    // Sanity check
    if len > 100 * 1024 * 1024 {
        // 100MB max
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Message too large",
        ));
    }

    // Read data
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data)?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_roundtrip() {
        let req = Request::execute(
            "INSERT INTO test VALUES (?, ?, ?)",
            vec![
                Value::Integer(42),
                Value::Text("hello".to_string()),
                Value::Null,
            ],
        );

        let serialized = req.serialize();
        let deserialized = Request::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.request_type, RequestType::Execute);
        assert_eq!(
            deserialized.sql,
            Some("INSERT INTO test VALUES (?, ?, ?)".to_string())
        );
        assert_eq!(deserialized.params.len(), 3);
        assert_eq!(deserialized.params[0], Value::Integer(42));
        assert_eq!(deserialized.params[1], Value::Text("hello".to_string()));
        assert_eq!(deserialized.params[2], Value::Null);
    }

    #[test]
    fn test_response_roundtrip() {
        let resp = Response::ok(5, 123);
        let serialized = resp.serialize();
        let deserialized = Response::deserialize(&serialized).unwrap();

        assert!(deserialized.is_ok());
        assert_eq!(deserialized.rows_affected, 5);
        assert_eq!(deserialized.last_insert_rowid, 123);
    }

    #[test]
    fn test_error_response_roundtrip() {
        let resp = Response::error("Something went wrong");
        let serialized = resp.serialize();
        let deserialized = Response::deserialize(&serialized).unwrap();

        assert!(!deserialized.is_ok());
        assert_eq!(
            deserialized.error_message,
            Some("Something went wrong".to_string())
        );
    }
}
