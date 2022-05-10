use std::fmt::{Display, Formatter};

pub enum DateIntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Display for DateIntervalUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DateIntervalUnit::Second => "SECOND",
            DateIntervalUnit::Minute => "MINUTE",
            DateIntervalUnit::Hour => "HOUR",
            DateIntervalUnit::Day => "DAY",
            DateIntervalUnit::Week => "WEEK",
            DateIntervalUnit::Month => "MONTH",
            DateIntervalUnit::Quarter => "QUARTER",
            DateIntervalUnit::Year => "YEAR",
        };

        write!(f, "{}", s)
    }
}
