#[derive(Clone, Copy, Debug)]
pub enum ParameterType {
    Null = 0,
    Integer = 1,
    String = 2,
    LargeObject = 3,
    Float = 4,
    Boolean = 5,
    Binary = 16,
    Ascii = 17,
}
