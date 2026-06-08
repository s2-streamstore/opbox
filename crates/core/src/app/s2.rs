use std::num::NonZeroU32;
use crate::types::WorkspaceId;
use eyre::eyre;
use s2_sdk::types::{BasinName, CreateStreamInput, RetryConfig, S2Config, S2Endpoints, S2Error, StreamName};
use s2_sdk::{S2, S2Basin};
use std::str::FromStr;

pub fn s2_client_from_env(access_token: &str) -> eyre::Result<S2> {
    let mut config = S2Config::new(access_token).with_retry(RetryConfig::new().with_max_attempts(NonZeroU32::new(1024).unwrap()));
    if let Ok(endpoints) = S2Endpoints::from_env() {
        config = config.with_endpoints(endpoints);
    }

    Ok(S2::new(config)?)
}

pub async fn s2_basin_from_env(basin: BasinName) -> eyre::Result<S2Basin> {
    let token = std::env::var("S2_ACCESS_TOKEN")
        .map_err(|_| eyre!("S2_ACCESS_TOKEN is not set; export an s2.dev access token"))?;
    let s2 = s2_client_from_env(&token)?;
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
