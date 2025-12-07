//! SQLite value types
//!
//! This module provides types for representing SQLite values.

use std::fmt;

/// A SQLite value
///
/// SQLite has a dynamic type system with 5 fundamental storage classes.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// NULL value
    Null,
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit IEEE floating point
    Real(f64),
    /// UTF-8 text string
    Text(String),
    /// Binary blob
    Blob(Vec<u8>),
}

impl Value {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get as an integer, if possible
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Real(f) => Some(*f as i64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get as a float, if possible
    pub fn as_real(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Real(f) => Some(*f),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get as a string, if possible
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Get as bytes, if possible
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            Value::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Get the type name
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Integer(_) => "INTEGER",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Blob(_) => "BLOB",
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "<blob {} bytes>", b.len()),
        }
    }
}

// Conversions from Rust types to Value

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<i8> for Value {
    fn from(i: i8) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<i16> for Value {
    fn from(i: i16) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<u8> for Value {
    fn from(i: u8) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<u16> for Value {
    fn from(i: u16) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<u32> for Value {
    fn from(i: u32) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Value::Real(f as f64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Real(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Integer(if b { 1 } else { 0 })
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<&String> for Value {
    fn from(s: &String) -> Self {
        Value::Text(s.clone())
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Blob(b)
    }
}

impl From<&[u8]> for Value {
    fn from(b: &[u8]) -> Self {
        Value::Blob(b.to_vec())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

// Conversions from Value to Rust types

impl TryFrom<Value> for i64 {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value.as_integer().ok_or("Cannot convert to integer")
    }
}

impl TryFrom<Value> for i32 {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value.as_integer().map(|i| i as i32).ok_or("Cannot convert to integer")
    }
}

impl TryFrom<Value> for f64 {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value.as_real().ok_or("Cannot convert to real")
    }
}

impl TryFrom<Value> for String {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(s),
            Value::Integer(i) => Ok(i.to_string()),
            Value::Real(f) => Ok(f.to_string()),
            Value::Null => Err("Cannot convert NULL to string"),
            Value::Blob(_) => Err("Cannot convert BLOB to string"),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Blob(b) => Ok(b),
            Value::Text(s) => Ok(s.into_bytes()),
            _ => Err("Cannot convert to blob"),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Integer(i) => Ok(i != 0),
            Value::Real(f) => Ok(f != 0.0),
            Value::Null => Ok(false),
            _ => Err("Cannot convert to bool"),
        }
    }
}

// Note: We don't implement TryFrom<Value> for Option<T> because it conflicts
// with the blanket impl in core. Use Value::as_* methods or manual conversion instead.

/// A wrapper for passing multiple parameters to queries
///
/// This is useful for passing a slice of values to execute().
#[derive(Debug, Clone)]
pub struct Params(pub Vec<Value>);

impl Params {
    /// Create new params
    pub fn new() -> Self {
        Params(Vec::new())
    }

    /// Add a parameter
    pub fn add<T: Into<Value>>(mut self, value: T) -> Self {
        self.0.push(value.into());
        self
    }
}

impl Default for Params {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for Params {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Into<Value>> FromIterator<T> for Params {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Params(iter.into_iter().map(|v| v.into()).collect())
    }
}

/// Macro for creating params
#[macro_export]
macro_rules! params {
    () => {
        $crate::Params::new()
    };
    ($($value:expr),+ $(,)?) => {
        {
            let mut p = $crate::Params::new();
            $(
                p = p.add($value);
            )+
            p
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::from(42i32), Value::Integer(42));
        assert_eq!(Value::from(3.14f64), Value::Real(3.14));
        assert_eq!(Value::from("hello"), Value::Text("hello".to_string()));
        assert_eq!(Value::from(vec![1u8, 2, 3]), Value::Blob(vec![1, 2, 3]));
        assert_eq!(Value::from(None::<i32>), Value::Null);
        assert_eq!(Value::from(Some(42i32)), Value::Integer(42));
    }

    #[test]
    fn test_value_as_methods() {
        let int_val = Value::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.as_real(), Some(42.0));

        let real_val = Value::Real(3.14);
        assert_eq!(real_val.as_real(), Some(3.14));
        assert_eq!(real_val.as_integer(), Some(3));

        let text_val = Value::Text("hello".to_string());
        assert_eq!(text_val.as_text(), Some("hello"));

        let null_val = Value::Null;
        assert!(null_val.is_null());
    }

    #[test]
    fn test_params_macro() {
        let p = params![1, "hello", 3.14];
        let values: Vec<Value> = p.into_iter().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Text("hello".to_string()));
        assert_eq!(values[2], Value::Real(3.14));
    }
}
