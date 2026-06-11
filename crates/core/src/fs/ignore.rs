use crate::fs::types::RelativePath;
use compact_str::CompactString;
use std::path::Path;

pub const METADATA_DIR_NAME: &str = ".opbox";
pub const IGNORE_FILE_NAME: &str = ".opboxignore";

const DEFAULT_IGNORE_FILE: &str = "\
# opbox always ignores .opbox/ internally.
# A bare name matches that file or directory at any depth.

# version control
.git

# build artifacts and dependency trees
target
node_modules
__pycache__
*.pyc

# editors and IDEs
.idea
*.swp
*.swo
*~
.#*

# operating system noise
.DS_Store
Thumbs.db
";

#[derive(Debug, Clone, Default)]
pub struct IgnoreRules {
    patterns: Vec<IgnorePattern>,
}

impl IgnoreRules {
    pub fn load(root: &Path) -> eyre::Result<Self> {
        let path = root.join(IGNORE_FILE_NAME);
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };

        Self::parse(&contents)
    }

    pub fn parse(contents: &str) -> eyre::Result<Self> {
        let mut patterns = Vec::new();
        for (idx, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            patterns.push(IgnorePattern::parse(line).map_err(|error| {
                eyre::eyre!(
                    "invalid {IGNORE_FILE_NAME} pattern on line {}: {error}",
                    idx + 1
                )
            })?);
        }

        Ok(Self { patterns })
    }

    pub fn is_ignored(&self, path: &RelativePath) -> bool {
        if path
            .as_components()
            .iter()
            .any(|component| component.as_str() == METADATA_DIR_NAME)
        {
            return true;
        }

        self.patterns
            .iter()
            .any(|pattern| pattern.matches_path(path))
    }
}

pub fn default_ignore_file_contents() -> &'static str {
    DEFAULT_IGNORE_FILE
}

#[derive(Debug, Clone)]
enum IgnorePattern {
    Component(ComponentPattern),
    Path(Vec<ComponentPattern>),
}

impl IgnorePattern {
    fn parse(value: &str) -> eyre::Result<Self> {
        let value = value.trim_matches('/');
        if value.is_empty() {
            eyre::bail!("pattern must not be empty");
        }
        if value.contains("**") {
            eyre::bail!("** is not supported yet");
        }

        let components = value
            .split('/')
            .map(ComponentPattern::new)
            .collect::<eyre::Result<Vec<_>>>()?;
        if components.len() == 1 {
            Ok(Self::Component(
                components.into_iter().next().expect("one component"),
            ))
        } else {
            Ok(Self::Path(components))
        }
    }

    fn matches_path(&self, path: &RelativePath) -> bool {
        match self {
            IgnorePattern::Component(pattern) => path
                .as_components()
                .iter()
                .any(|component| pattern.matches(component.as_str())),
            IgnorePattern::Path(patterns) => {
                let components = path.as_components();
                components.len() >= patterns.len()
                    && components
                        .iter()
                        .zip(patterns)
                        .all(|(component, pattern)| pattern.matches(component.as_str()))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ComponentPattern {
    raw: CompactString,
}

impl ComponentPattern {
    fn new(value: &str) -> eyre::Result<Self> {
        if value.is_empty() {
            eyre::bail!("path component pattern must not be empty");
        }
        if value == "." || value == ".." {
            eyre::bail!("path component pattern must not be {value:?}");
        }
        Ok(Self {
            raw: CompactString::from(value),
        })
    }

    fn matches(&self, value: &str) -> bool {
        wildcard_match(self.raw.as_str(), value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> RelativePath {
        RelativePath::parse(value).expect("valid path")
    }

    #[test]
    fn default_ignore_file_parses_and_matches_common_junk() -> eyre::Result<()> {
        let rules = IgnoreRules::parse(default_ignore_file_contents())?;

        for ignored in [
            "target/debug/build/foo.d",
            "sub/project/target/release/app",
            "web/node_modules/lodash/index.js",
            "lib/__pycache__/mod.cpython-312.pyc",
            "notes/.#draft.org",
            "notes/draft.org~",
            ".idea/workspace.xml",
            "photos/Thumbs.db",
        ] {
            assert!(
                rules.is_ignored(&path(ignored)),
                "{ignored} must be ignored"
            );
        }

        for kept in [
            "notes/targets.md",
            "target.txt",
            "docs/ideas.md",
            "src/main.rs",
        ] {
            assert!(!rules.is_ignored(&path(kept)), "{kept} must not be ignored");
        }
        Ok(())
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_idx, mut value_idx) = (0, 0);
    let mut star_idx = None;
    let mut star_value_idx = 0;

    while value_idx < value.len() {
        if pattern_idx < pattern.len()
            && (pattern[pattern_idx] == value[value_idx] || pattern[pattern_idx] == b'?')
        {
            pattern_idx += 1;
            value_idx += 1;
        } else if pattern_idx < pattern.len() && pattern[pattern_idx] == b'*' {
            star_idx = Some(pattern_idx);
            pattern_idx += 1;
            star_value_idx = value_idx;
        } else if let Some(star) = star_idx {
            pattern_idx = star + 1;
            star_value_idx += 1;
            value_idx = star_value_idx;
        } else {
            return false;
        }
    }

    while pattern_idx < pattern.len() && pattern[pattern_idx] == b'*' {
        pattern_idx += 1;
    }

    pattern_idx == pattern.len()
}
