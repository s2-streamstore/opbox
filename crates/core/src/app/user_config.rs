use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const OPBOX_CONFIG_DIR_ENV: &str = "OPBOX_CONFIG_DIR";
const CONFIG_DIR_NAME: &str = "opbox";
const CONFIG_FILE_NAME: &str = "config.toml";

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum UserConfigKey {
    Basin,
    DefaultBasin,
    AccessToken,
    AccountEndpoint,
    BasinEndpoint,
    DaemonLogLevel,
    ClientLogLevel,
}

impl UserConfigKey {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct UserConfig {
    pub basin: Option<String>,
    pub default_basin: Option<String>,
    pub access_token: Option<String>,
    pub account_endpoint: Option<String>,
    pub basin_endpoint: Option<String>,
    pub daemon_log_level: Option<String>,
    pub client_log_level: Option<String>,
}

impl UserConfig {
    pub fn get(&self, key: UserConfigKey) -> Option<&str> {
        match key {
            UserConfigKey::Basin => self.basin.as_deref(),
            UserConfigKey::DefaultBasin => self.default_basin.as_deref(),
            UserConfigKey::AccessToken => self.access_token.as_deref(),
            UserConfigKey::AccountEndpoint => self.account_endpoint.as_deref(),
            UserConfigKey::BasinEndpoint => self.basin_endpoint.as_deref(),
            UserConfigKey::DaemonLogLevel => self.daemon_log_level.as_deref(),
            UserConfigKey::ClientLogLevel => self.client_log_level.as_deref(),
        }
    }

    pub fn set(&mut self, key: UserConfigKey, value: String) {
        match key {
            UserConfigKey::Basin => self.basin = Some(value),
            UserConfigKey::DefaultBasin => self.default_basin = Some(value),
            UserConfigKey::AccessToken => self.access_token = Some(value),
            UserConfigKey::AccountEndpoint => self.account_endpoint = Some(value),
            UserConfigKey::BasinEndpoint => self.basin_endpoint = Some(value),
            UserConfigKey::DaemonLogLevel => self.daemon_log_level = Some(value),
            UserConfigKey::ClientLogLevel => self.client_log_level = Some(value),
        }
    }

    pub fn unset(&mut self, key: UserConfigKey) {
        match key {
            UserConfigKey::Basin => self.basin = None,
            UserConfigKey::DefaultBasin => self.default_basin = None,
            UserConfigKey::AccessToken => self.access_token = None,
            UserConfigKey::AccountEndpoint => self.account_endpoint = None,
            UserConfigKey::BasinEndpoint => self.basin_endpoint = None,
            UserConfigKey::DaemonLogLevel => self.daemon_log_level = None,
            UserConfigKey::ClientLogLevel => self.client_log_level = None,
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (UserConfigKey, &str)> {
        [
            UserConfigKey::Basin,
            UserConfigKey::DefaultBasin,
            UserConfigKey::AccessToken,
            UserConfigKey::AccountEndpoint,
            UserConfigKey::BasinEndpoint,
            UserConfigKey::DaemonLogLevel,
            UserConfigKey::ClientLogLevel,
        ]
        .into_iter()
        .filter_map(|key| self.get(key).map(|value| (key, value)))
    }
}

pub fn user_config_dir() -> eyre::Result<PathBuf> {
    if let Some(path) = std::env::var_os(OPBOX_CONFIG_DIR_ENV) {
        return Ok(PathBuf::from(path));
    }

    let base = dirs::config_dir()
        .ok_or_else(|| eyre::eyre!("could not determine user config directory"))?;
    Ok(base.join(CONFIG_DIR_NAME))
}

pub fn user_config_path() -> eyre::Result<PathBuf> {
    Ok(user_config_dir()?.join(CONFIG_FILE_NAME))
}

pub fn load_user_config() -> eyre::Result<UserConfig> {
    load_user_config_from_path(&user_config_path()?)
}

pub fn load_user_config_from_path(path: &Path) -> eyre::Result<UserConfig> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UserConfig::default());
        }
        Err(error) => eyre::bail!("failed to read {}: {error}", path.display()),
    };

    toml::from_str(&contents).map_err(|error| eyre::eyre!("{}: {error}", path.display()))
}

pub fn save_user_config(config: &UserConfig) -> eyre::Result<PathBuf> {
    let path = user_config_path()?;
    save_user_config_to_path(config, &path)?;
    Ok(path)
}

pub fn save_user_config_to_path(config: &UserConfig, path: &Path) -> eyre::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| eyre::eyre!("failed to create {}: {error}", parent.display()))?;
        secure_config_dir(parent)?;
    }

    let body = toml::to_string_pretty(config)?;
    write_config_file(path, &body)
        .map_err(|error| eyre::eyre!("failed to write {}: {error}", path.display()))
}

#[cfg(unix)]
fn secure_config_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn secure_config_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn write_config_file(path: &Path, body: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(body.as_bytes())
}

#[cfg(not(unix))]
fn write_config_file(path: &Path, body: &str) -> std::io::Result<()> {
    std::fs::write(path, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_user_config_with_private_permissions() -> eyre::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opbox-user-config-test-{}", rand::random::<u64>()));
        let path = root.join("config.toml");
        let config = UserConfig {
            basin: Some("workspace-basin".to_string()),
            default_basin: Some("test-basin".to_string()),
            access_token: Some("tok-123".to_string()),
            account_endpoint: Some("account.s2.test".to_string()),
            basin_endpoint: Some("{basin}.s2.test".to_string()),
            daemon_log_level: Some("opbox_core=debug".to_string()),
            client_log_level: Some("warn".to_string()),
        };

        save_user_config_to_path(&config, &path)?;
        let loaded = load_user_config_from_path(&path)?;
        assert_eq!(loaded.basin.as_deref(), Some("workspace-basin"));
        assert_eq!(loaded.default_basin.as_deref(), Some("test-basin"));
        assert_eq!(loaded.access_token.as_deref(), Some("tok-123"));
        assert_eq!(loaded.account_endpoint.as_deref(), Some("account.s2.test"));
        assert_eq!(loaded.basin_endpoint.as_deref(), Some("{basin}.s2.test"));
        assert_eq!(loaded.daemon_log_level.as_deref(), Some("opbox_core=debug"));
        assert_eq!(loaded.client_log_level.as_deref(), Some("warn"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let dir_mode = std::fs::metadata(&root)?.permissions().mode() & 0o777;
            let file_mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700);
            assert_eq!(file_mode, 0o600);
        }

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
}
