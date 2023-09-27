//! Catalog layer.

use std::sync::Arc;

use super::{Catalog, CatalogRef};

/// Catalog layer
pub trait CatalogLayer {
    /// Result of layering a catalog.
    type LayeredCatalog: Catalog;

    /// Layering a catalog.
    fn layer(&self, catalog: CatalogRef) -> Arc<Self::LayeredCatalog>;
}
