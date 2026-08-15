//! Generated Fleet agent protocol types and service contracts.

/// Version 1 of the Fleet agent protocol.
#[allow(missing_docs)]
#[allow(clippy::clone_on_ref_ptr)]
pub mod v1 {
    tonic::include_proto!("ventstream.fleet.v1");
}

#[cfg(test)]
mod tests {
    use super::v1::{DesiredRunState, OperationKind};

    #[test]
    fn safety_relevant_enums_reserve_zero_for_unspecified() {
        assert_eq!(DesiredRunState::Unspecified as i32, 0);
        assert_ne!(DesiredRunState::Running as i32, 0);
        assert_eq!(OperationKind::Unspecified as i32, 0);
        assert_ne!(OperationKind::Rebootstrap as i32, 0);
    }
}
