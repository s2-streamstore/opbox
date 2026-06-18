use crate::types::WorkspaceId;
use eyre::eyre;
use s2_sdk::types::{
    AccountEndpoint, BasinEndpoint, BasinName, CreateStreamInput, RetryConfig, S2Config,
    S2Endpoints, S2Error, StreamName,
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
    pub fn from_env() -> eyre::Result<Self> {
        let access_token = required_env("S2_ACCESS_TOKEN")?;
        Ok(Self {
            access_token,
            account_endpoint: optional_env("S2_ACCOUNT_ENDPOINT")?,
            basin_endpoint: optional_env("S2_BASIN_ENDPOINT")?,
        })
    }

    pub fn from_env_or_user_config(user_config: &UserConfig) -> eyre::Result<Self> {
        let access_token = optional_env("S2_ACCESS_TOKEN")?
            .or_else(|| user_config.access_token.clone())
            .ok_or_else(|| {
                eyre!(
                    "S2_ACCESS_TOKEN is not set and opbox user config has no access-token; \
                     run `ob config set access-token <token>` or export S2_ACCESS_TOKEN"
                )
            })?;
        Ok(Self {
            access_token,
            account_endpoint: optional_env("S2_ACCOUNT_ENDPOINT")?
                .or_else(|| user_config.account_endpoint.clone()),
            basin_endpoint: optional_env("S2_BASIN_ENDPOINT")?
                .or_else(|| user_config.basin_endpoint.clone()),
        })
    }

    pub fn from_env_workspace_or_user_config(
        workspace_account_endpoint: Option<&str>,
        workspace_basin_endpoint: Option<&str>,
        user_config: &UserConfig,
    ) -> eyre::Result<Self> {
        let access_token = optional_env("S2_ACCESS_TOKEN")?
            .or_else(|| user_config.access_token.clone())
            .ok_or_else(|| {
                eyre!(
                    "S2_ACCESS_TOKEN is not set and opbox user config has no access-token; \
                     run `ob config set access-token <token>` or export S2_ACCESS_TOKEN"
                )
            })?;
        Ok(Self {
            access_token,
            account_endpoint: optional_env("S2_ACCOUNT_ENDPOINT")?
                .or_else(|| workspace_account_endpoint.map(str::to_owned))
                .or_else(|| user_config.account_endpoint.clone()),
            basin_endpoint: optional_env("S2_BASIN_ENDPOINT")?
                .or_else(|| workspace_basin_endpoint.map(str::to_owned))
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

fn required_env(key: &str) -> eyre::Result<String> {
    optional_env(key)?.ok_or_else(|| {
        eyre!("{key} is not set; export it or run `ob config set access-token <token>`")
    })
}

fn optional_env(key: &str) -> eyre::Result<Option<String>> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => eyre::bail!("{key} is not valid unicode"),
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

pub fn s2_client_from_env(access_token: &str) -> eyre::Result<S2> {
    s2_client_from_config(&S2ConnectionConfig {
        access_token: access_token.to_string(),
        account_endpoint: optional_env("S2_ACCOUNT_ENDPOINT")?,
        basin_endpoint: optional_env("S2_BASIN_ENDPOINT")?,
    })
}

pub async fn s2_basin_from_config(
    basin: BasinName,
    connection: &S2ConnectionConfig,
) -> eyre::Result<S2Basin> {
    let s2 = s2_client_from_config(connection)?;
    Ok(s2.basin(basin))
}

pub async fn s2_basin_from_env(basin: BasinName) -> eyre::Result<S2Basin> {
    let connection = S2ConnectionConfig::from_env()?;
    s2_basin_from_config(basin, &connection).await
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
