use std::collections::HashMap;

use opendal::{Operator, Scheme};
use url::Url;

use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Operator args: root
pub const OP_ARGS_ROOT: &str = "root";
/// Operator args: bucket
pub const OP_ARGS_BUCKET: &str = "bucket";
/// s3 endpoint
pub const OP_ARGS_ENDPOINT: &str = "endpoint";
/// s3 region
pub const OP_ARGS_REGION: &str = "region";
/// s3 access key
pub const OP_ARGS_ACCESS_KEY: &str = "access_key_id";
/// s3 access secret
pub const OP_ARGS_ACCESS_SECRET: &str = "secret_access_key";

/// Args for creating opendal operator
#[derive(Debug, Clone)]
pub struct OperatorArgs {
    scheme: Scheme,
    args: HashMap<String, String>,
}

impl OperatorArgs {
    /// Create a builder with `Scheme`
    pub fn builder(scheme: Scheme) -> OperatorArgsBuilder {
        OperatorArgsBuilder(OperatorArgs {
            scheme,
            args: HashMap::new(),
        })
    }

    /// Creates a builder from path.
    pub fn builder_from_path(path: &str) -> Result<OperatorArgsBuilder> {
        if path.starts_with('/') {
            // Local file path such as: /tmp
            return Ok(OperatorArgs::builder(Scheme::Fs).with_arg(OP_ARGS_ROOT, path));
        }

        let url = Url::parse(path)?;

        let op = match url.scheme() {
            "file" => {
                OperatorArgs::builder(Scheme::Fs).with_arg(OP_ARGS_ROOT, url.path().to_string())
            }
            "s3" | "s3a" => OperatorArgs::builder(Scheme::S3)
                .with_arg(OP_ARGS_ROOT, url.path().to_string())
                .with_arg(
                    OP_ARGS_BUCKET,
                    url.host_str()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                format!("Invalid s3 url: {path}"),
                            )
                        })?
                        .to_string(),
                ),
            _ => {
                return Err(Error::new(
                    ErrorKind::IcebergFeatureUnsupported,
                    format!("Unsupported warehouse path: {path}"),
                ));
            }
        };

        Ok(op)
    }
}

/// Operator args builder.
pub struct OperatorArgsBuilder(OperatorArgs);

impl OperatorArgsBuilder {
    /// Add arg.
    pub fn with_arg(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.0.args.insert(key.to_string(), value.to_string());
        self
    }

    /// Add all args
    pub fn with_args(mut self, args: impl Iterator<Item = (impl ToString, impl ToString)>) -> Self {
        self.0
            .args
            .extend(args.map(|(k, v)| (k.to_string(), v.to_string())));
        self
    }

    /// Build arg.
    pub fn build(self) -> OperatorArgs {
        self.0
    }
}

impl TryFrom<&OperatorArgs> for Operator {
    type Error = Error;

    fn try_from(args: &OperatorArgs) -> Result<Self> {
        Ok(Operator::via_map(args.scheme, args.args.clone())?)
    }
}

impl OperatorArgs {
    /// Creates a subdir of current operator args.
    pub fn sub_dir(&self, sub_dir: &str) -> Result<Self> {
        let op = Operator::try_from(self)?;

        let mut new_args = self.clone();
        let new_root = format!("{}/{sub_dir}", op.info().root());
        new_args.args.insert(OP_ARGS_ROOT.to_string(), new_root);

        Ok(new_args)
    }

    /// Returns root path of current operator args.
    pub fn url(&self) -> Result<String> {
        let op = Operator::try_from(self)?;
        Ok(format!(
            "{}://{}/{}",
            self.scheme,
            op.info().name(),
            op.info().root()
        ))
    }
}
