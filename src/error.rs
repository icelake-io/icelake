use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// Result that is a wrapper of `Result<T, opendal::Error>`
pub type Result<T> = std::result::Result<T, Error>;

/// ErrorKind is all kinds of Error of opendal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// IceLake don't know what happened here, and no actions other than
    /// just returning it back. For example, opendal returns an internal
    /// service error.
    Unexpected,

    /// Iceberg data is invalid.
    ///
    /// This error is returned when we try to read a table from iceberg but
    /// failed to parse it's metadata or data file correctly.
    ///
    /// The table could be invalid or corrupted.
    IcebergDataInvalid,
    /// Iceberg feature is not supported.
    ///
    /// This error is returned when given iceberg feature is not supported.
    IcebergFeatureUnsupported,
}

impl ErrorKind {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<ErrorKind> for &'static str {
    fn from(v: ErrorKind) -> &'static str {
        match v {
            ErrorKind::Unexpected => "Unexpected",
            ErrorKind::IcebergDataInvalid => "IcebergDataInvalid",
            ErrorKind::IcebergFeatureUnsupported => "IcebergFeatureUnsupported",
        }
    }
}

/// Error is the error struct returned by all icelake functions.
pub struct Error {
    kind: ErrorKind,
    message: String,

    context: Vec<(&'static str, String)>,
    source: Option<anyhow::Error>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("context", &self.context);
            de.field("source", &self.source);
            return de.finish();
        }

        write!(f, "{}", self.kind)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "    {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source: {source:?}")?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

impl Error {
    /// Create a new Error with error kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),

            context: Vec::default(),
            source: None,
        }
    }

    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    /// Set source for error.
    ///
    /// # Notes
    ///
    /// If the source has been set, we will raise a panic here.
    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(src.into());
        self
    }

    /// Return error's kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<apache_avro::Error> for Error {
    fn from(v: apache_avro::Error) -> Self {
        Self::new(ErrorKind::Unexpected, "handling avro data failed").set_source(v)
    }
}

impl From<serde_json::Error> for Error {
    fn from(v: serde_json::Error) -> Self {
        Self::new(ErrorKind::Unexpected, "handling json data failed").set_source(v)
    }
}

impl From<opendal::Error> for Error {
    fn from(v: opendal::Error) -> Self {
        Self::new(ErrorKind::Unexpected, "IO operation failed").set_source(v)
    }
}

#[cfg(feature = "io_parquet")]
impl From<parquet::errors::ParquetError> for Error {
    fn from(v: parquet::errors::ParquetError) -> Self {
        Self::new(ErrorKind::Unexpected, "handling parquet data failed").set_source(v)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use once_cell::sync::Lazy;

    use super::*;

    static TEST_ERROR: Lazy<Error> = Lazy::new(|| Error {
        kind: ErrorKind::Unexpected,
        message: "something wrong happened".to_string(),
        context: vec![
            ("path", "/path/to/file".to_string()),
            ("called", "send_async".to_string()),
        ],
        source: Some(anyhow!("networking error")),
    });

    #[test]
    fn test_error_display() {
        let s = format!("{}", Lazy::force(&TEST_ERROR));
        assert_eq!(
            s,
            r#"Unexpected, context: { path: /path/to/file, called: send_async } => something wrong happened, source: networking error"#
        )
    }

    #[test]
    fn test_error_debug() {
        let s = format!("{:?}", Lazy::force(&TEST_ERROR));
        assert_eq!(
            s,
            r#"Unexpected => something wrong happened

Context:
    path: /path/to/file
    called: send_async

Source: networking error
"#
        )
    }
}
