use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("wrong number of arguments")]
    WrongArgCount,
    #[error("value is not an integer or out of range")]
    NotAnInteger,
    #[error("EXEC without MULTI")]
    ExecWithoutMulti,
    #[error("MULTI calls can not be nested")]
    NestedMulti,
    #[error("DISCARD without MULTI")]
    DiscardWithoutMulti,
    #[error("value is not a valid float")]
    InvalidFloat,
    #[error("invalid longitude,latitude pair")]
    InvalidCoordinates,
    #[error("could not find the requested member")]
    MemberNotFound,
}
