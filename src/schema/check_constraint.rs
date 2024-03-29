#[derive(Clone, Debug, PartialEq)]
pub enum CheckConstraint {
    Literal(String),
    EqString(String),
    NotEqString(String),
    MinInt(i64),
    MaxInt(i64),
    MinFloat(f64),
    MaxFloat(f64),
}

impl Eq for CheckConstraint {}
