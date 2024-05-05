use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error occurred")]
    IoError(#[from] std::io::Error),
    #[error("could not serialize")]
    SerializeError(#[from] SerializeError),
    #[error("could not deserialize")]
    DeserializeError(#[from] DeserializeError),
}

/// a wrapper because bincode errors do not differentiate
/// betweeen serialization and deserialization
#[derive(Debug, Error)]
pub struct SerializeError {
    pub msg: String,
    #[source]
    pub source: Box<bincode::ErrorKind>,
}

impl std::fmt::Display for SerializeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.msg, self.source)
    }
}

/// a wrapper because bincode errors do not differentiate
/// betweeen serialization and deserialization
#[derive(Debug, Error)]
pub struct DeserializeError {
    pub msg: String,
    #[source]
    pub source: Box<bincode::ErrorKind>,
}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.msg, self.source)
    }
}
