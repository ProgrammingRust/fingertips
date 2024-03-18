use std::error::Error;

/// Result type that is being returned from methods that can fail and thus have [`FingertipsError`]s.
pub type FingertipsResult<T> = Result<T, FingertipsError>;

/// Errors that can result from Fingertips.
// [`Error`] is public, but opaque and easy to keep compatible.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct FingertipsError(#[from] FingertipsErrorKind);

// Accessors for anything we do want to expose publicly.
impl FingertipsError {
    /// Expose the inner error kind.
    ///
    /// This is useful for matching on the error kind.
    pub fn into_inner(self) -> FingertipsErrorKind {
        self.0
    }
}

/// [`FingertipsErrorKind`] describes the errors that can happen while executing a high-level command.
///
/// This is a non-exhaustive enum, so additional variants may be added in future. It is
/// recommended to match against the wildcard `_` instead of listing all possible variants,
/// to avoid problems when new variants are added.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum FingertipsErrorKind {
    /// An error occurred while reading from or writing to a file.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// An error occurred while parsing a file
    TermEmpty,
    /// An error occured in the algorithm
    AlgorithmError,
    /// No entry to move
    NoEntryToMove,
    /// Computer not big enough to hold index entry, you may be on 32bit platform
    PlatformLimitExceeded,
}

trait FingertipsErrorMarker: Error {}

// impl FingertipsErrorMarker for FingertipErrorsInTheCodeBase {}

impl<E> From<E> for FingertipsError
where
    E: FingertipsErrorMarker,
    FingertipsErrorKind: From<E>,
{
    fn from(value: E) -> Self {
        Self(FingertipsErrorKind::from(value))
    }
}
