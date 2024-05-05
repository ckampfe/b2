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

#[derive(Debug, Error)]
pub struct SerializeError {
    pub msg: String,
    #[source] // optional if field name is `source`
    pub source: Box<bincode::ErrorKind>,
}

impl std::fmt::Display for SerializeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.msg, self.source)
    }
}

#[derive(Debug, Error)]
pub struct DeserializeError {
    pub msg: String,
    #[source] // optional if field name is `source`
    pub source: Box<bincode::ErrorKind>,
}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.msg, self.source)
    }
}
