use futures::stream::{FuturesOrdered, StreamExt};
use async_trait::async_trait;

#[async_trait]
pub trait Client {
    async fn get_object(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> anyhow::Result<near_indexer_primitives::StreamerMessage>;

    async fn list_objects(
        &self,
        start_after: &crate::types::BlockHeight,
    ) -> anyhow::Result<Vec<near_indexer_primitives::StreamerMessage>>;
}

#[derive(Clone, Debug)]
pub struct FastNearClient {
    endpoint: String,
    client: reqwest::Client
}

impl FastNearClient {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new()
        }
    }
}

#[async_trait]
impl Client for FastNearClient {
    async fn get_object(
        &self,
        block_height: crate::types::BlockHeight,
    ) -> anyhow::Result<near_indexer_primitives::StreamerMessage> {
        let result = self.client
            .get(&format!("{}/v0/block/{}", self.endpoint, block_height))
            .send()
            .await?
            .json()
            .await?;
        Ok(serde_json::from_value::<near_indexer_primitives::StreamerMessage>(result)?)
    }

    async fn list_objects(
        &self,
        start_after: &crate::types::BlockHeight,
    ) -> anyhow::Result<Vec<near_indexer_primitives::StreamerMessage>> {
        let mut futures = FuturesOrdered::new();
        futures.extend((start_after.clone()..=start_after.clone()+400).map(|block_height| {
            self.get_object(block_height)
        }));
        let mut result = vec![];
        while let Some(block) = futures.next().await {
            match block {
                Ok(block) => result.push(block),
                Err(err) => {
                    tracing::debug!(
                        target: crate::LAKE_FRAMEWORK,
                        "Failed to fetch block, skipping\n{:#?}",
                        err
                    );
                    continue;
                }
            }

        }
        Ok(result)
    }
}

/// Queries the list of the objects in the bucket, grouped by "/" delimiter.
/// Returns the list of block heights that can be fetched
pub async fn list_block_heights(
    lake_s3_client: &impl Client,
    start_from_block_height: crate::types::BlockHeight,
) -> anyhow::Result<Vec<near_indexer_primitives::StreamerMessage>> {
    tracing::debug!(
        target: crate::LAKE_FRAMEWORK,
        "Fetching block heights from S3, after #{}...",
        start_from_block_height
    );
    lake_s3_client
        .list_objects(&start_from_block_height)
        .await
}

/// Fetches the block data JSON from AWS S3 and returns the `BlockView`
pub async fn fetch_block(
    fast_near_client: &impl Client,
    block_height: crate::types::BlockHeight,
) -> anyhow::Result<near_indexer_primitives::views::BlockView> {
    let body_bytes = fast_near_client
        .get_object(block_height)
        .await?;

    Ok(body_bytes.block)
}

/// Fetches the block data JSON from AWS S3 and returns the `BlockView` retrying until it succeeds (indefinitely)
pub async fn fetch_block_or_retry(
    fast_near_client: &impl Client,
    block_height: crate::types::BlockHeight,
) -> anyhow::Result<near_indexer_primitives::views::BlockView> {
    loop {
        match fetch_block(fast_near_client, block_height).await {
            Ok(block_view) => break Ok(block_view),
            Err(err) => {
                    tracing::debug!(
                        target: crate::LAKE_FRAMEWORK,
                        "Failed to fetch block #{}, retrying immediately\n{:#?}",
                        block_height,
                        err
                    );
                }
        }
    }
}

/// Fetches the shard data JSON from AWS S3 and returns the `IndexerShard`
pub async fn fetch_shard(
    lake_s3_client: &impl Client,
    block_height: crate::types::BlockHeight,
    shard_id: u64,
) -> anyhow::Result<near_indexer_primitives::IndexerShard> {
    let body_bytes = lake_s3_client
        .get_object(block_height)
        .await?;
    Ok(body_bytes.shards.get(shard_id as usize).unwrap().clone())
}

/// Fetches the shard data JSON from AWS S3 and returns the `IndexerShard`
pub async fn fetch_shard_or_retry(
    lake_s3_client: &impl Client,
    block_height: crate::types::BlockHeight,
    shard_id: u64,
) -> anyhow::Result<near_indexer_primitives::IndexerShard> {
    loop {
        match fetch_shard(lake_s3_client, block_height, shard_id).await {
            Ok(shard) => break Ok(shard),
            Err(err) => {
                    tracing::debug!(
                        target: crate::LAKE_FRAMEWORK,
                        "Failed to fetch shard {} of block #{}, retrying immediately\n{:#?}",
                        shard_id,
                        block_height,
                        err
                    );
                }
        }
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//
//     use async_trait::async_trait;
//
//     use aws_sdk_s3::operation::get_object::GetObjectOutput;
//     use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
//     use aws_sdk_s3::primitives::ByteStream;
//
//     use aws_smithy_types::body::SdkBody;
//
//     #[derive(Clone, Debug)]
//     pub struct LakeS3Client {}
//
//     #[async_trait]
//     impl S3Client for LakeS3Client {
//         async fn get_object(
//             &self,
//             _bucket: &str,
//             prefix: &str,
//         ) -> Result<
//             aws_sdk_s3::operation::get_object::GetObjectOutput,
//             aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
//         > {
//             let path = format!("{}/blocks/{}", env!("CARGO_MANIFEST_DIR"), prefix);
//             let file_bytes = tokio::fs::read(path).await.unwrap();
//             let stream = ByteStream::new(SdkBody::from(file_bytes));
//             Ok(GetObjectOutput::builder().body(stream).build())
//         }
//
//         async fn list_objects(
//             &self,
//             _bucket: &str,
//             _start_after: &str,
//         ) -> Result<
//             aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output,
//             aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error>,
//         > {
//             Ok(ListObjectsV2Output::builder().build())
//         }
//     }
//
//     #[tokio::test]
//     async fn deserializes_meta_transactions() {
//         let lake_client = LakeS3Client {};
//
//         let streamer_message =
//             fetch_streamer_message(&lake_client, "near-lake-data-mainnet", 879765)
//                 .await
//                 .unwrap();
//
//         let delegate_action = &streamer_message.shards[0]
//             .chunk
//             .as_ref()
//             .unwrap()
//             .transactions[0]
//             .transaction
//             .actions[0];
//
//         assert_eq!(
//             serde_json::to_value(delegate_action).unwrap(),
//             serde_json::json!({
//                 "Delegate": {
//                     "delegate_action": {
//                         "sender_id": "test.near",
//                         "receiver_id": "test.near",
//                         "actions": [
//                           {
//                             "AddKey": {
//                               "public_key": "ed25519:CnQMksXTTtn81WdDujsEMQgKUMkFvDJaAjDeDLTxVrsg",
//                               "access_key": {
//                                 "nonce": 0,
//                                 "permission": "FullAccess"
//                               }
//                             }
//                           }
//                         ],
//                         "nonce": 879546,
//                         "max_block_height": 100,
//                         "public_key": "ed25519:8Rn4FJeeRYcrLbcrAQNFVgvbZ2FCEQjgydbXwqBwF1ib"
//                     },
//                     "signature": "ed25519:25uGrsJNU3fVgUpPad3rGJRy2XQum8gJxLRjKFCbd7gymXwUxQ9r3tuyBCD6To7SX5oSJ2ScJZejwqK1ju8WdZfS"
//                 }
//             })
//         );
//     }
// }
