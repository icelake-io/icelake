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

/// OperatorCreator is used to create an opendal::Operator.
///
/// OpenDAL doesn't support checkout sub dir yet, thus we need to support create a new operator
/// with sub dir. There is an ongoing issue at upstream but no progress yet: https://github.com/apache/incubator-opendal/issues/3151
/// Instead of waiting for upstream, we can create a new operator with sub dir by ourselves.
/// We can decide to migrate to upstream's solution in the future.
///
/// Besides, our users of icelake could implement their config parsers that not fully compatible
/// with iceberg based config like `iceberg.io.xxx`. So we make it a trait instead to allow users
/// to implement their own `OperatorCreator` instead a full storage catalog.
pub trait OperatorCreator: Send + Sync + 'static {
    /// Create an operator with existing config.
    ///
    /// Implementor could cache the operator instead to avoid creating it every time.
    fn create(&self) -> Result<Operator>;

    /// Create an operator with sub dir.
    fn create_with_subdir(&self, path: &str) -> Result<Operator>;
}

/// Args for creating opendal operator
#[derive(Debug, Clone)]
pub struct IcebergTableIoArgs {
    scheme: Scheme,
    args: HashMap<String, String>,
}

impl IcebergTableIoArgs {
    /// Create a builder with `Scheme`
    pub fn builder(scheme: Scheme) -> IcebergTableIoArgsBuilder {
        IcebergTableIoArgsBuilder {
            scheme,
            args: HashMap::new(),
        }
    }

    /// Creates a builder from path.
    pub fn builder_from_path(path: &str) -> Result<IcebergTableIoArgsBuilder> {
        if path.starts_with('/') {
            // Local file path such as: /tmp
            return Ok(IcebergTableIoArgs::builder(Scheme::Fs).with_arg(OP_ARGS_ROOT, path));
        }

        let url = Url::parse(path)?;

        let op = match url.scheme() {
            "file" => IcebergTableIoArgs::builder(Scheme::Fs)
                .with_arg(OP_ARGS_ROOT, url.path().to_string()),
            "s3" | "s3a" => IcebergTableIoArgs::builder(Scheme::S3)
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
pub struct IcebergTableIoArgsBuilder {
    scheme: Scheme,
    args: HashMap<String, String>,
}

impl IcebergTableIoArgsBuilder {
    /// Add arg.
    pub fn with_arg(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.args.insert(key.to_string(), value.to_string());
        self
    }

    /// Add all args
    pub fn with_args(mut self, args: impl Iterator<Item = (impl ToString, impl ToString)>) -> Self {
        self.args
            .extend(args.map(|(k, v)| (k.to_string(), v.to_string())));
        self
    }

    /// Build arg.
    pub fn build(self) -> Result<IcebergTableIoArgs> {
        Ok(IcebergTableIoArgs {
            scheme: self.scheme,
            args: self.args,
        })
    }
}

impl OperatorCreator for IcebergTableIoArgs {
    fn create(&self) -> Result<Operator> {
        let op = Operator::via_map(self.scheme, self.args.clone())?;
        // let prometheus_layer = PrometheusLayer::with_registry(self.registry.clone());
        // op = op.layer(prometheus_layer);
        Ok(op)
    }

    fn create_with_subdir(&self, path: &str) -> Result<Operator> {
        let scheme = self.scheme;
        let mut args = self.args.clone();

        let new_root = format!(
            "{}/{path}",
            args.get(OP_ARGS_ROOT).cloned().unwrap_or_default()
        );
        args.insert(OP_ARGS_ROOT.to_string(), new_root);

        Ok(Operator::via_map(scheme, args.clone())?)
    }
}

impl IcebergTableIoArgs {
    /// Returns root path of current operator args.
    pub fn url(&self) -> Result<String> {
        let op = self.create()?;
        Ok(format!(
            "{}://{}/{}",
            self.scheme,
            op.info().name(),
            op.info().root()
        ))
    }
}
