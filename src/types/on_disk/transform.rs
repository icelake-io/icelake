use crate::types;

#[cfg(test)]
mod tests {
    use crate::types::Transform;
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
            let actual = input.parse::<Transform>().unwrap();

            assert_eq!(actual, expected, "transform is not match for {input}")
        }
    }
}
