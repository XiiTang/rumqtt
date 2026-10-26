//! Passive observations of publications removed by explicit recovery.

/// A publication removed by native recovery without a peer acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationRetirement {
    pub packet_id: u16,
    pub reason: PublicationRetirementReason,
}

/// The existing native decision that removed a publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationRetirementReason {
    NotTransmitted,
    MessageExpired { expiry_interval_seconds: u32 },
}
