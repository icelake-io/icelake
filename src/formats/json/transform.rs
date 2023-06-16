use crate::types;
use anyhow::anyhow;
use anyhow::Result;

/// Parse transform string represent into types::Transform enum.
pub fn parse_transform(s: &str) -> Result<types::Transform> {
    let t = match s {
        "identity" => types::Transform::Identity,
        "year" => types::Transform::Year,
        "month" => types::Transform::Month,
        "day" => types::Transform::Day,
        "hour" => types::Transform::Hour,
        "void" => types::Transform::Void,
        v if v.starts_with("bucket") => {
            let length = v
                .strip_prefix("bucket")
                .expect("transform must starts with `bucket`")
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .map_err(|err| anyhow!("transform bucket type {v:?} is invalid: {err:?}"))?;

            types::Transform::Bucket(length)
        }
        v if v.starts_with("truncate") => {
            let width = v
                .strip_prefix("truncate")
                .expect("transform must starts with `truncate`")
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .map_err(|err| anyhow!("transform truncate type {v:?} is invalid: {err:?}"))?;

            types::Transform::Truncate(width)
        }
        v => return Err(anyhow!("transform {:?} is not valid transform", v)),
    };

    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_transform() {
        let cases = vec![
            ("identity", types::Transform::Identity),
            ("bucket[2]", types::Transform::Bucket(2)),
            ("bucket[100000]", types::Transform::Bucket(100000)),
            ("truncate[1]", types::Transform::Truncate(1)),
            ("truncate[445324]", types::Transform::Truncate(445324)),
            ("year", types::Transform::Year),
            ("month", types::Transform::Month),
            ("day", types::Transform::Day),
            ("hour", types::Transform::Hour),
            ("void", types::Transform::Void),
        ];

        for (input, expected) in cases {
            let actual = parse_transform(input).unwrap();

            assert_eq!(actual, expected, "transform is not match for {input}")
        }
    }
}
