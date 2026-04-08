use thiserror::Error;

#[derive(Debug, Error)]
pub enum XAddError {
    #[error("The ID specified in XADD must be greater than 0-0")]
    ZeroId,
    #[error("The ID specified in XADD is equal or smaller than the target stream top item")]
    NotIncremental,
    #[error("Invalid stream ID format")]
    InvalidFormat,
}

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
