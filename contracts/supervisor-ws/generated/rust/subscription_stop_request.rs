// SubscriptionStopRequest represents a SubscriptionStopRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SubscriptionStopRequest {
    #[serde(rename="subscription_id")]
    pub subscription_id: String,
}
