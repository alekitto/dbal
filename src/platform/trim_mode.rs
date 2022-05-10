#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TrimMode {
    Unspecified,
    Leading,
    Trailing,
    Both,
}
