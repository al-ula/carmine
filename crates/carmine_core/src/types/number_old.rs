use std::cmp::Ordering;

use redb::{Key, TypeName, Value};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Number(pub f64);

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<f64> for Number {
    fn from(v: f64) -> Self {
        Number(v)
    }
}

impl From<Number> for f64 {
    fn from(v: Number) -> f64 {
        v.0
    }
}

impl std::ops::Add for Number {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Number(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Number {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Number(self.0 - rhs.0)
    }
}

impl std::ops::Mul for Number {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        Number(self.0 * rhs.0)
    }
}

impl std::ops::Div for Number {
    type Output = Self;
    fn div(self, rhs: Self) -> Self {
        Number(self.0 / rhs.0)
    }
}

impl std::ops::Add<f64> for Number {
    type Output = Self;
    fn add(self, rhs: f64) -> Self {
        Number(self.0 + rhs)
    }
}

impl std::ops::Sub<f64> for Number {
    type Output = Self;
    fn sub(self, rhs: f64) -> Self {
        Number(self.0 - rhs)
    }
}

impl std::ops::Mul<f64> for Number {
    type Output = Self;
    fn mul(self, rhs: f64) -> Self {
        Number(self.0 * rhs)
    }
}

impl std::ops::Div<f64> for Number {
    type Output = Self;
    fn div(self, rhs: f64) -> Self {
        Number(self.0 / rhs)
    }
}

impl std::ops::AddAssign for Number {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl std::ops::SubAssign for Number {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl std::ops::MulAssign for Number {
    fn mul_assign(&mut self, rhs: Self) {
        self.0 *= rhs.0;
    }
}

impl std::ops::DivAssign for Number {
    fn div_assign(&mut self, rhs: Self) {
        self.0 /= rhs.0;
    }
}

impl Number {
    #[inline]
    pub fn into_inner(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }
    #[inline]
    pub fn sqrt(self) -> Self {
        Self(self.0.sqrt())
    }
    #[inline]
    pub fn sin(self) -> Self {
        Self(self.0.sin())
    }
    #[inline]
    pub fn cos(self) -> Self {
        Self(self.0.cos())
    }
    #[inline]
    pub fn exp(self) -> Self {
        Self(self.0.exp())
    }
    #[inline]
    pub fn ln(self) -> Self {
        Self(self.0.ln())
    }
    #[inline]
    pub fn floor(self) -> Self {
        Self(self.0.floor())
    }
    #[inline]
    pub fn ceil(self) -> Self {
        Self(self.0.ceil())
    }
    #[inline]
    pub fn round(self) -> Self {
        Self(self.0.round())
    }
    #[inline]
    pub fn to_degrees(self) -> Self {
        Self(self.0.to_degrees())
    }
    #[inline]
    pub fn to_radians(self) -> Self {
        Self(self.0.to_radians())
    }

    #[inline]
    pub fn powf(self, x: f64) -> Self {
        Self(self.0.powf(x))
    }
    #[inline]
    pub fn powi(self, n: i32) -> Self {
        Self(self.0.powi(n))
    }
    #[inline]
    pub fn hypot(self, other: f64) -> Self {
        Self(self.0.hypot(other))
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
        Number(f64::from_le_bytes(arr))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("your_crate::Number")
    }
}

impl Key for Number {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a: [u8; 8] = data1
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .expect("invalid Number key length");
        let b: [u8; 8] = data2
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .expect("invalid Number key length");

        let f1 = f64::from_le_bytes(a);
        let f2 = f64::from_le_bytes(b);

        f1.total_cmp(&f2)
    }
}

#[cfg(test)]
mod tests {
    use super::Number;
    use redb::{Key, Value};

    #[test]
    fn test_newtype_basic_ops() {
        let a = Number(2.0);
        let b = Number(3.0);

        assert_eq!((a + b).into_inner(), 5.0);
        assert_eq!((b - a).into_inner(), 1.0);
        assert_eq!((a * b).into_inner(), 6.0);
        assert_eq!((b / a).into_inner(), 1.5);
    }

    #[test]
    fn test_forward_methods() {
        let x = Number(9.0);
        assert_eq!(x.sqrt().into_inner(), 3.0);

        let y = Number(2.0);
        assert_eq!(y.powi(3).into_inner(), 8.0);
        assert_eq!(y.powf(4.0).into_inner(), 16.0);
    }

    #[test]
    fn test_value_roundtrip() {
        let n = Number(123.456_f64);
        let bytes = Number::as_bytes(&n);
        let m = Number::from_bytes(&bytes);
        assert_eq!(n, m);
    }

    #[test]
    fn test_key_compare() {
        let a = Number(1.5);
        let b = Number(2.5);

        let ba = Number::as_bytes(&a);
        let bb = Number::as_bytes(&b);

        use std::cmp::Ordering::*;
        assert_eq!(Number::compare(&ba, &bb), Less);
        assert_eq!(Number::compare(&bb, &ba), Greater);
        assert_eq!(Number::compare(&ba, &ba), Equal);
    }

    #[test]
    fn test_negative_cmp() {
        let a = Number(-10.0);
        let b = Number(0.0);

        let ba = Number::as_bytes(&a);
        let bb = Number::as_bytes(&b);

        use std::cmp::Ordering::*;
        assert_eq!(Number::compare(&ba, &bb), Less);
    }

    #[test]
    fn test_nan_behaviour() {
        let nan = Number(f64::NAN);
        let zero = Number(0.0);

        let bn = Number::as_bytes(&nan);
        let bz = Number::as_bytes(&zero);

        // total_cmp ensures deterministic ordering
        assert!(matches!(
            Number::compare(&bn, &bz),
            std::cmp::Ordering::Greater | std::cmp::Ordering::Less
        ));
    }
}
