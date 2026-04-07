use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisError {
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
}
