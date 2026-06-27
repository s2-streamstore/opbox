use crate::fs::types::RelativePath;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use std::path::Path;

pub const METADATA_DIR_NAME: &str = ".opbox";
pub const IGNORE_FILE_NAME: &str = ".opboxignore";

const DEFAULT_IGNORE_FILE: &str = "\
# opbox always ignores .opbox/ internally.
# .opboxignore uses gitignore syntax.
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

#[derive(Debug, Clone)]
pub struct IgnoreRules {
    matcher: Gitignore,
}

impl Default for IgnoreRules {
    fn default() -> Self {
        Self {
            matcher: Gitignore::empty(),
        }
    }
}

impl IgnoreRules {
    pub fn load(root: &Path) -> eyre::Result<Self> {
        let path = root.join(IGNORE_FILE_NAME);
        let mut builder = GitignoreBuilder::new(root);
        match builder.add(&path) {
            None => {}
            Some(error) if path.exists() => return Err(error.into()),
            Some(_) => {}
        };

        Self::from_builder(builder)
    }

    pub fn parse(contents: &str) -> eyre::Result<Self> {
        Self::parse_for_root(Path::new(""), contents)
    }

    pub fn parse_for_root(root: &Path, contents: &str) -> eyre::Result<Self> {
        let mut builder = GitignoreBuilder::new(root);
        for (idx, line) in contents.lines().enumerate() {
            builder.add_line(None, line).map_err(|error| {
                eyre::eyre!(
                    "invalid {IGNORE_FILE_NAME} pattern on line {}: {error}",
                    idx + 1
                )
            })?;
        }

        Self::from_builder(builder)
    }

    fn from_builder(builder: GitignoreBuilder) -> eyre::Result<Self> {
        Ok(Self {
            matcher: builder.build()?,
        })
    }

    pub fn is_ignored(&self, path: &RelativePath, is_dir: bool) -> bool {
        if is_hard_ignored(path) {
            return true;
        }

        self.matcher
            .matched_path_or_any_parents(path.to_db_path(), is_dir)
            .is_ignore()
    }
}

pub fn default_ignore_file_contents() -> &'static str {
    DEFAULT_IGNORE_FILE
}

pub fn is_hard_ignored(path: &RelativePath) -> bool {
    path.as_components().iter().any(|component| {
        let component = component.as_str();
        component == METADATA_DIR_NAME || is_projection_temp_component(component)
    })
}

fn is_projection_temp_component(component: &str) -> bool {
    component.starts_with('.') && component.contains(".opbox-tmp-")
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
            ".note.txt.opbox-tmp-0123456789abcdef",
            "nested/.note.txt.opbox-tmp-0123456789abcdef",
        ] {
            assert!(
                rules.is_ignored(&path(ignored), false),
                "{ignored} must be ignored"
            );
        }

        for kept in [
            "notes/targets.md",
            "target.txt",
            "docs/ideas.md",
            "src/main.rs",
        ] {
            assert!(
                !rules.is_ignored(&path(kept), false),
                "{kept} must not be ignored"
            );
        }
        Ok(())
    }

    #[test]
    fn opboxignore_uses_gitignore_syntax() -> eyre::Result<()> {
        let rules = IgnoreRules::parse(
            "\
dist/
!dist/keep.txt
foo/**/bar
*.log
",
        )?;

        assert!(rules.is_ignored(&path("dist"), true));
        assert!(!rules.is_ignored(&path("dist"), false));
        assert!(!rules.is_ignored(&path("dist/keep.txt"), false));
        assert!(rules.is_ignored(&path("dist/drop.txt"), false));
        assert!(rules.is_ignored(&path("foo/a/b/bar"), false));
        assert!(rules.is_ignored(&path("nested/error.log"), false));
        assert!(!rules.is_ignored(&path("nested/error.txt"), false));

        Ok(())
    }

    #[test]
    fn hard_ignores_cannot_be_unignored() -> eyre::Result<()> {
        let rules = IgnoreRules::parse(
            "\
!.opbox/**
!*.opbox-tmp-*
",
        )?;

        assert!(rules.is_ignored(&path(".opbox/storage.db"), false));
        assert!(rules.is_ignored(&path(".note.txt.opbox-tmp-0123456789abcdef"), false));
        Ok(())
    }
}
