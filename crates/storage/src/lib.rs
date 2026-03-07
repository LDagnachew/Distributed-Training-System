/// Minimal helpers for working with S3-style URIs.
///
/// This is intentionally lightweight for the MVP and does not depend on any
/// AWS SDK crates yet. It can be shared by both coordinator and workers.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Uri {
    pub bucket: String,
    pub key: String,
}

impl S3Uri {
    /// Parse a URI of the form `s3://bucket/key...`.
    pub fn parse(uri: &str) -> Result<Self, String> {
        const PREFIX: &str = "s3://";

        if !uri.starts_with(PREFIX) {
            return Err(format!("expected URI to start with `{PREFIX}`, got `{uri}`"));
        }

        let without_scheme = &uri[PREFIX.len()..];
        let mut parts = without_scheme.splitn(2, '/');
        let bucket = parts
            .next()
            .ok_or_else(|| format!("missing bucket in S3 URI `{uri}`"))?;
        let key = parts
            .next()
            .unwrap_or("")
            .to_string();

        if bucket.is_empty() {
            return Err(format!("bucket is empty in S3 URI `{uri}`"));
        }

        Ok(S3Uri {
            bucket: bucket.to_string(),
            key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_s3_uri() {
        let parsed = S3Uri::parse("s3://my-bucket/path/to/object").unwrap();
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.key, "path/to/object");
    }

    #[test]
    fn parses_bucket_root() {
        let parsed = S3Uri::parse("s3://my-bucket/").unwrap();
        assert_eq!(parsed.bucket, "my-bucket");
        assert_eq!(parsed.key, "");
    }
}
