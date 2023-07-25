#[cfg(test)]
mod tests {
    use crate::types::Transform;

    #[test]
    fn test_parse_transform() {
        let cases = vec![
            ("identity", Transform::Identity),
            ("bucket[2]", Transform::Bucket(2)),
            ("bucket[100000]", Transform::Bucket(100000)),
            ("truncate[1]", Transform::Truncate(1)),
            ("truncate[445324]", Transform::Truncate(445324)),
            ("year", Transform::Year),
            ("month", Transform::Month),
            ("day", Transform::Day),
            ("hour", Transform::Hour),
            ("void", Transform::Void),
        ];

        for (input, expected) in cases {
            let actual = input.parse::<Transform>().unwrap();

            assert_eq!(actual, expected, "transform is not match for {input}")
        }
    }
}
