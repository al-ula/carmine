use redb::{Key, TypeName, Value};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Number(pub [u8; 8]);

// Encodes f64 into 8 bytes that sort correctly (lexicographically)
pub fn encode_f64_key(val: f64) -> [u8; 8] {
    // 1. Normalize -0.0 to 0.0 (Optional but recommended for IDs)
    //    Otherwise -0.0 and 0.0 will be two different keys.
    let val = if val == 0.0 { 0.0 } else { val };

    // 2. Get the raw bits
    let bits = val.to_bits();

    // 3. Create the mask
    //    If sign bit is 1 (negative): mask is ALL ONES (flip everything)
    //    If sign bit is 0 (positive): mask is SIGN BIT ONLY (flip sign)
    let msb = 1u64 << 63;
    let mask = if bits & msb != 0 {
        !0 // u64::MAX
    } else {
        msb
    };

    // 4. Apply mask and convert to Big Endian bytes
    //    Big Endian is MANDATORY for byte-level sorting
    (bits ^ mask).to_be_bytes()
}

// Decodes the bytes back to f64 (for when you read the key back)
pub fn decode_f64_key(bytes: [u8; 8]) -> f64 {
    let bits = u64::from_be_bytes(bytes);
    let msb = 1u64 << 63;

    // We need to reverse the mask logic.
    // If the first bit is 0, it WAS negative (we flipped 1 -> 0).
    // If the first bit is 1, it WAS positive (we flipped 0 -> 1).
    let mask = if bits & msb == 0 { !0 } else { msb };

    f64::from_bits(bits ^ mask)
}

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&decode_f64_key(self.0), f)
    }
}

impl From<f64> for Number {
    fn from(v: f64) -> Self {
        Number(encode_f64_key(v))
    }
}

impl From<Number> for f64 {
    fn from(v: Number) -> f64 {
        decode_f64_key(v.0)
    }
}

impl Value for Number {
    type SelfType<'a>
        = Number
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let arr: [u8; 8] = data
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .expect("invalid Number bytes length");
        Number(arr)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0
    }

    fn type_name() -> TypeName {
        TypeName::new("carmine_core::types::Number")
    }
}

impl Key for Number {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let a: [u8; 8] = data1
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .expect("invalid Number key length");
        let b: [u8; 8] = data2
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .expect("invalid Number key length");

        // Since the bytes are already encoded with encode_f64_key,
        // they are designed to sort correctly lexicographically
        a.cmp(&b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_roundtrip() {
        let n = Number::from(123.456_f64);
        let bytes = Number::as_bytes(&n);
        let m = Number::from_bytes(&bytes);
        assert_eq!(n, m);
        assert_eq!(f64::from(m), 123.456_f64);
    }

    #[test]
    fn test_key_compare() {
        let a = Number::from(1.5);
        let b = Number::from(2.5);

        let ba = Number::as_bytes(&a);
        let bb = Number::as_bytes(&b);

        use std::cmp::Ordering::*;
        assert_eq!(Number::compare(&ba, &bb), Less);
        assert_eq!(Number::compare(&bb, &ba), Greater);
        assert_eq!(Number::compare(&ba, &ba), Equal);
    }

    #[test]
    fn test_negative_cmp() {
        let a = Number::from(-10.0);
        let b = Number::from(0.0);

        let ba = Number::as_bytes(&a);
        let bb = Number::as_bytes(&b);

        use std::cmp::Ordering::*;
        assert_eq!(Number::compare(&ba, &bb), Less);
    }

    #[test]
    fn test_negative_ordering() {
        let numbers = vec![-100.0, -10.0, -1.0, 0.0, 1.0, 10.0, 100.0];
        let mut encoded: Vec<_> = numbers
            .iter()
            .map(|&n| Number::from(n))
            .map(|n| Number::as_bytes(&n))
            .collect();

        // Sort the encoded bytes lexicographically
        encoded.sort_by(|a, b| a.cmp(b));

        // Decode and verify the order is correct
        let decoded: Vec<f64> = encoded
            .iter()
            .map(|bytes| Number::from_bytes(bytes))
            .map(|n| f64::from(n))
            .collect();

        assert_eq!(decoded, numbers);
    }

    #[test]
    fn test_nan_behaviour() {
        let nan = Number::from(f64::NAN);
        let zero = Number::from(0.0);

        let bn = Number::as_bytes(&nan);
        let bz = Number::as_bytes(&zero);

        // total_cmp ensures deterministic ordering
        // NaN should be greater than all other values
        assert!(matches!(
            Number::compare(&bn, &bz),
            std::cmp::Ordering::Greater
        ));
    }

    #[test]
    fn test_mixed_range_ordering() {
        let values = vec![
            f64::MIN,
            -1e10,
            -1000.0,
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            1000.0,
            1e10,
            f64::MAX,
        ];

        let mut encoded_with_original: Vec<_> =
            values.iter().map(|&v| (Number::from(v), v)).collect();

        // Sort by the encoded bytes
        encoded_with_original.sort_by(|a, b| {
            let bytes_a = Number::as_bytes(&a.0);
            let bytes_b = Number::as_bytes(&b.0);
            bytes_a.cmp(&bytes_b)
        });

        // Extract the original values
        let sorted_values: Vec<f64> = encoded_with_original.iter().map(|&(_, v)| v).collect();

        assert_eq!(sorted_values, values);
    }
}
