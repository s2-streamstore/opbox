use crate::app::control::{DaemonWarning, StreamRetentionSummary};
use crate::types::WorkspaceId;
use eyre::eyre;
use s2_sdk::types::{
    AccountEndpoint, BasinConfig, BasinEndpoint, BasinName, BasinReconfiguration,
    CreateStreamInput, EncryptionAlgorithm, ReconfigureBasinInput, RetentionPolicy, RetryConfig,
    S2Config, S2Endpoints, S2Error, StreamConfig, StreamName,
};
use s2_sdk::{S2, S2Basin};
use std::num::NonZeroU32;
use std::str::FromStr;
use tracing::warn;

use super::user_config::UserConfig;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S2ConnectionConfig {
    pub access_token: String,
    pub account_endpoint: Option<String>,
    pub basin_endpoint: Option<String>,
}

impl S2ConnectionConfig {
    pub fn from_user_config(user_config: &UserConfig) -> eyre::Result<Self> {
        Self::from_workspace_or_user_config(&UserConfig::default(), None, None, user_config)
    }

    pub fn from_workspace_or_user_config(
        workspace_config: &UserConfig,
        metadata_account_endpoint: Option<&str>,
        metadata_basin_endpoint: Option<&str>,
        user_config: &UserConfig,
    ) -> eyre::Result<Self> {
        let access_token = workspace_config
            .access_token
            .clone()
            .or_else(|| user_config.access_token.clone())
            .ok_or_else(|| {
                eyre!(
                    "no S2 access token configured; run `ob config set access-token <token>` \
                     or `ob config --workspace set access-token <token>`"
                )
            })?;
        Ok(Self {
            access_token,
            account_endpoint: workspace_config
                .account_endpoint
                .clone()
                .or_else(|| metadata_account_endpoint.map(str::to_owned))
                .or_else(|| user_config.account_endpoint.clone()),
            basin_endpoint: workspace_config
                .basin_endpoint
                .clone()
                .or_else(|| metadata_basin_endpoint.map(str::to_owned))
                .or_else(|| user_config.basin_endpoint.clone()),
        })
    }

    pub fn endpoint_pair_for_metadata(&self) -> (Option<String>, Option<String>) {
        match (&self.account_endpoint, &self.basin_endpoint) {
            (Some(account_endpoint), Some(basin_endpoint)) => {
                (Some(account_endpoint.clone()), Some(basin_endpoint.clone()))
            }
            _ => (None, None),
        }
    }
}

pub fn s2_client_from_config(connection: &S2ConnectionConfig) -> eyre::Result<S2> {
    let mut config = S2Config::new(&connection.access_token)
        .with_retry(RetryConfig::new().with_max_attempts(NonZeroU32::new(5).unwrap()));

    match (&connection.account_endpoint, &connection.basin_endpoint) {
        (Some(account_endpoint), Some(basin_endpoint)) => {
            let account_endpoint = AccountEndpoint::new(account_endpoint)?;
            let basin_endpoint = BasinEndpoint::new(basin_endpoint)?;
            config = config.with_endpoints(S2Endpoints::new(account_endpoint, basin_endpoint)?);
        }
        (Some(_), None) => {
            warn!(
                "S2 account endpoint is set but basin endpoint is not; using default S2 endpoints"
            );
        }
        (None, Some(_)) => {
            warn!(
                "S2 basin endpoint is set but account endpoint is not; using default S2 endpoints"
            );
        }
        (None, None) => {}
    }

    Ok(S2::new(config)?)
}

#[allow(unreachable_patterns)]
pub fn s2_error_is_connectivity(error: &S2Error) -> bool {
    match error {
        S2Error::Client(message) => {
            let message = message.to_ascii_lowercase();
            !message.contains("malformed access token")
        }
        S2Error::Server(err) => matches!(
            err.code.as_str(),
            "client_hangup"
                | "hot_server"
                | "other"
                | "rate_limited"
                | "request_timeout"
                | "storage"
                | "transaction_conflict"
                | "unavailable"
                | "upstream_timeout"
        ),
        S2Error::Validation(_) | S2Error::AppendConditionFailed(_) | S2Error::ReadUnwritten(_) => {
            false
        }
        _ => false,
    }
}

pub fn s2_error_is_encryption(error: &S2Error) -> bool {
    matches!(
        error,
        S2Error::Server(err)
            if matches!(err.code.as_str(), "decryption_failed" | "bad_header")
                && (err.message.contains("encryption key")
                    || err.message.contains("decryption"))
    )
}

pub fn wrap_s2_encryption_error(error: S2Error) -> eyre::Report {
    if s2_error_is_encryption(&error) {
        eyre::eyre!(
            "encryption error: the configured cipher does not match this workspace's encryption key\n\
             detail: {error}"
        )
    } else {
        error.into()
    }
}

pub fn s2_error_is_not_found(error: &S2Error) -> bool {
    matches!(
        error,
        S2Error::Server(err)
            if matches!(err.code.as_str(), "basin_not_found" | "stream_not_found")
    )
}

pub fn report_is_s2_connectivity(error: &eyre::Report) -> bool {
    error
        .downcast_ref::<S2Error>()
        .is_some_and(s2_error_is_connectivity)
}

pub async fn s2_basin_from_config(
    basin: BasinName,
    connection: &S2ConnectionConfig,
) -> eyre::Result<S2Basin> {
    let s2 = s2_client_from_config(connection)?;
    Ok(s2.basin(basin))
}

pub fn ops_stream_name(workspace_id: &WorkspaceId) -> eyre::Result<StreamName> {
    StreamName::from_str(&format!("{}/ops", workspace_id.0)).map_err(|err| {
        eyre!(
            "invalid ops stream name for workspace {}: {err}",
            workspace_id.0
        )
    })
}

pub async fn create_workspace_stream(
    s2_basin: &S2Basin,
    workspace_id: &WorkspaceId,
) -> eyre::Result<()> {
    let stream_name = ops_stream_name(workspace_id)?;
    match s2_basin
        .create_stream(CreateStreamInput::new(stream_name))
        .await
    {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "resource_already_exists" => {
            Err(eyre!("workspace {} already exists", workspace_id.0))
        }
        Err(err) => Err(err.into()),
    }
}

pub async fn ensure_workspace_stream_exists(
    s2_basin: &S2Basin,
    workspace_id: &WorkspaceId,
) -> eyre::Result<()> {
    let stream_name = ops_stream_name(workspace_id)?;
    let stream = s2_basin.stream(stream_name);
    match stream.check_tail().await {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "stream_not_found" => {
            Err(eyre!("workspace {} does not exist", workspace_id.0))
        }
        Err(err) => Err(err.into()),
    }
}

pub async fn workspace_stream_retention_warning(
    s2_basin: &S2Basin,
    workspace_id: &WorkspaceId,
) -> eyre::Result<Option<DaemonWarning>> {
    let stream_name = ops_stream_name(workspace_id)?;
    let config = s2_basin.get_stream_config(stream_name).await?;
    Ok(stream_config_retention_warning(&config))
}

pub fn stream_config_retention_warning(config: &StreamConfig) -> Option<DaemonWarning> {
    retention_warning_summary(config.retention_policy)
        .map(|retention| DaemonWarning::OpsStreamRetentionNotInfinite { retention })
}

pub async fn basin_default_stream_retention_warning(
    s2: &S2,
    basin: BasinName,
) -> eyre::Result<Option<DaemonWarning>> {
    let config = s2.get_basin_config(basin).await?;
    Ok(basin_config_retention_warning(&config))
}

pub async fn ensure_basin_stream_cipher(s2: &S2, basin: BasinName) -> eyre::Result<bool> {
    let config = s2.get_basin_config(basin.clone()).await?;
    if config.stream_cipher.is_some() {
        return Ok(true);
    }
    match s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin.clone(),
            BasinReconfiguration::new().with_stream_cipher(EncryptionAlgorithm::Aes256Gcm),
        ))
        .await
    {
        Ok(_) => Ok(true),
        Err(e) => {
            tracing::warn!(
                "could not configure encryption on basin '{basin}': {e}. \
                 Workspace will be created without encryption. \
                 To enable encryption, ask a basin admin to set stream_cipher to aes-256-gcm."
            );
            Ok(false)
        }
    }
}

pub fn basin_config_retention_warning(config: &BasinConfig) -> Option<DaemonWarning> {
    let retention_policy = config
        .default_stream_config
        .as_ref()
        .and_then(|config| config.retention_policy);

    retention_warning_summary(retention_policy)
        .map(|retention| DaemonWarning::BasinDefaultStreamRetentionNotInfinite { retention })
}

fn retention_warning_summary(
    retention_policy: Option<RetentionPolicy>,
) -> Option<StreamRetentionSummary> {
    match retention_policy {
        Some(RetentionPolicy::Infinite) => None,
        Some(RetentionPolicy::Age(seconds)) => Some(StreamRetentionSummary::Age { seconds }),
        None => Some(StreamRetentionSummary::Unspecified),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_config_prefers_workspace_then_metadata_then_user_config() -> eyre::Result<()> {
        let user_config = UserConfig {
            access_token: Some("global-token".to_string()),
            account_endpoint: Some("global-account.s2.test".to_string()),
            basin_endpoint: Some("{basin}.global.s2.test".to_string()),
            ..UserConfig::default()
        };
        let workspace_config = UserConfig {
            access_token: Some("workspace-token".to_string()),
            account_endpoint: Some("workspace-account.s2.test".to_string()),
            ..UserConfig::default()
        };

        let connection = S2ConnectionConfig::from_workspace_or_user_config(
            &workspace_config,
            Some("metadata-account.s2.test"),
            Some("{basin}.metadata.s2.test"),
            &user_config,
        )?;

        assert_eq!(connection.access_token, "workspace-token");
        assert_eq!(
            connection.account_endpoint.as_deref(),
            Some("workspace-account.s2.test")
        );
        assert_eq!(
            connection.basin_endpoint.as_deref(),
            Some("{basin}.metadata.s2.test")
        );

        Ok(())
    }

    #[test]
    fn connection_config_requires_access_token() {
        let error = S2ConnectionConfig::from_workspace_or_user_config(
            &UserConfig::default(),
            None,
            None,
            &UserConfig::default(),
        )
        .expect_err("missing access token should fail");

        assert!(
            error.to_string().contains("no S2 access token configured"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn stream_config_retention_warning_flags_non_infinite_retention() {
        assert_eq!(
            stream_config_retention_warning(
                &StreamConfig::new().with_retention_policy(RetentionPolicy::Infinite)
            ),
            None
        );

        assert_eq!(
            stream_config_retention_warning(
                &StreamConfig::new().with_retention_policy(RetentionPolicy::Age(86_400))
            ),
            Some(DaemonWarning::OpsStreamRetentionNotInfinite {
                retention: StreamRetentionSummary::Age { seconds: 86_400 }
            })
        );

        assert_eq!(
            stream_config_retention_warning(&StreamConfig::new()),
            Some(DaemonWarning::OpsStreamRetentionNotInfinite {
                retention: StreamRetentionSummary::Unspecified
            })
        );
    }

    #[test]
    fn basin_config_retention_warning_flags_non_infinite_default_stream_retention() {
        assert_eq!(
            basin_config_retention_warning(&BasinConfig::new().with_default_stream_config(
                StreamConfig::new().with_retention_policy(RetentionPolicy::Infinite)
            )),
            None
        );

        assert_eq!(
            basin_config_retention_warning(&BasinConfig::new().with_default_stream_config(
                StreamConfig::new().with_retention_policy(RetentionPolicy::Age(3_600))
            )),
            Some(DaemonWarning::BasinDefaultStreamRetentionNotInfinite {
                retention: StreamRetentionSummary::Age { seconds: 3_600 }
            })
        );

        assert_eq!(
            basin_config_retention_warning(&BasinConfig::new()),
            Some(DaemonWarning::BasinDefaultStreamRetentionNotInfinite {
                retention: StreamRetentionSummary::Unspecified
            })
        );
    }
}
