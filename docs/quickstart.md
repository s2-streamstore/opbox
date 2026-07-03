# Quickstart

## Prerequisites

### Install opbox

#### Install script (recommended)

```bash
curl -fsSL https://opbox.dev/install.sh | bash
```

This installs the latest release binaries (`ob` and `opbox-daemon`) into `~/.local/bin`. Feel free to [read the script](https://opbox.dev/install.sh) before running it.

Options such as `OPBOX_VERSION`, `OPBOX_INSTALL_DIR`, and `OPBOX_INSTALL_FROM_SOURCE=1` are documented at the top of the script.

#### From a release archive

Download an archive for your platform from [GitHub releases](https://github.com/s2-streamstore/opbox/releases), then place both `ob` and `opbox-daemon` in the same directory on your `$PATH`.

#### From source

```bash
cargo install --locked --path crates/client
cargo install --locked --path crates/daemon
```

You should have `ob` and `opbox-daemon` in your `$PATH` now.

## S2 configuration

S2 is used as the shared journal across opbox daemons. This is how CRDT ops are shared.

You can use the hosted service at s2.dev, or run `s2-lite` yourself.

### Using `s2.dev`

Go to [s2.dev](https://s2.dev), make an account, and create an access token.

> [!NOTE]
> Do put down a payment method in order to be able to make streams with infinite retention. The free tier (no payment method listed) is restricted to 28 day data retention. In other words, your workspace will break after 4 weeks unless you do this.

Hold on to the access token. You can accept the defaults when creating it. Don't share it with anyone else.

Next, create a basin, making sure that:
- The retention policy is set to `Infinite` (age-based TTLs will work for short-lived workspaces)

![create_basin.png](images/create_basin.png)

If you have the `s2` CLI installed, you could also use it to create a basin:

```bash
s2 create-basin \
  my-opbox-basin \
  --retention-policy infinite
```

Configure your local opbox using your access token and basin name:

```bash
ob config set access-token "MY_TOKEN"
ob config set default-basin "MY_BASIN"
```

`ob config` writes to an OS user-level opbox config file by default. These values become the defaults for every opbox workspace you create or clone as this OS user. Use `ob config --workspace ...` inside a workspace when one workspace needs its own basin, access token, endpoints, or daemon log level.

At this point, you're set.

### Using `s2-lite`

If you want to run S2 yourself, use [`s2-lite`](https://github.com/s2-streamstore/s2#s2-lite). This assumes you have the `s2` CLI installed.

Your S2 instance needs to be reachable by every opbox daemon that should sync through it.

For a quick local test, run `s2-lite` on the same machine as your opbox workspaces and point opbox at `localhost`.

```bash
# leave this running in one terminal
s2 lite
```

In each terminal where you want to run `s2` or `ob`, set:

```bash
export S2_ACCOUNT_ENDPOINT=http://localhost:80
export S2_BASIN_ENDPOINT=http://localhost:80
export S2_ACCESS_TOKEN=ignored
export S2_BASIN=my-test-basin
```

`S2_ACCESS_TOKEN=ignored` is intentional: `s2-lite` does not check access tokens, but the SDK still expects a value.

If `s2-lite` is running on another host or port, replace both endpoint URLs with the address your opbox daemons can reach.

Create a basin:

```bash
s2 create-basin \
  $S2_BASIN \
  --retention-policy infinite
```

`ob init` and `ob clone` also read these `S2_*` environment variables. For a throwaway local test, that is usually enough.

If you would rather persist the s2-lite settings in opbox config, use:

```bash
ob config set default-basin "$S2_BASIN"
ob config set access-token "$S2_ACCESS_TOKEN"
ob config set account-endpoint "$S2_ACCOUNT_ENDPOINT"
ob config set basin-endpoint "$S2_BASIN_ENDPOINT"
```

## Create your first workspace

You can use an existing directory, or create a new one. I'll assume the latter for now.

```bash
mkdir -p ~/my-opbox-workspace
cd ~/my-opbox-workspace

# init the workspace
ob init
```

You should see something like this:

```console
me@mac my-opbox-workspace % ob init
initialized opbox workspace
  basin          my-opbox-basin 
  root           /Users/me/my-opbox-workspace
  cipher         89abcdefghjkmnpqrstvwxyz23456789abcdefghjkmnpqrstuvw

your workspace is: wersq5ks6776xwqhdpycs835g4w6pg7z

  share token    opbox-wersq5ks6776xwqhdpycs835g4w6pg7z-bootstrap

share this clone command (contains limited access token and workspace cipher):

  ob clone \
    --workspace wersq5ks6776xwqhdpycs835g4w6pg7z \
    --access-token I7oAAAAAAABqRXQ5hghQl6Kc8xJtVmqcc5k5Skpnzg6jVKew \
    --cipher 89abcdefghjkmnpqrstvwxyz23456789abcdefghjkmnpqrstuvw \
    --basin my-opbox-basin 

run ob start to begin syncing
```

Great, it worked.

At this point, the workspace has been created, and an initial snapshot has been successfully sent to S2.

Anyone who wants to sync can clone this workspace using the command printed above.

> [!TIP]
> 
> The `access-token` printed in the `ob clone` command is created during initialization, and constrained to the current workspace. It's not a global access token.
> 
> Sharing it will not allow others to create new workspaces using your account. The `cipher` is the workspace encryption key; share it only with people who should be able to decrypt workspace contents.
> 
> You can create per-user share tokens, revoke tokens, and list all with `ob share`.

To listen for local changes and apply remote changes, start the daemon:
```bash
ob start
```

> [!TIP]
> Most `ob` commands operate on the local workspace. If your `$PWD` is not in a workspace directory (or a subdirectory of it), they won't work. Similar to `git`.
>
> `ob config` is user-wide by default. Add `--workspace` to read or write `.opbox/config.toml` for the current workspace.

## Cloning an existing workspace

> [!NOTE]
> Make sure your opbox config is correct. If you did the S2 setup steps, send the access token, cipher, and basin to anyone you want to share your workspace with. They can set the S2 values globally with `ob config`, or for one clone with `ob clone --workspace ... --cipher ... --basin ... --access-token ...`.

This will likely be done on another computer.

```bash
mkdir -p ~/my-opbox-workspace-clone-1
cd ~/my-opbox-workspace-clone-1

# the directory must be empty to start
# then, use the workspace id from earlier
ob clone \
  --workspace wersq5ks6776xwqhdpycs835g4w6pg7z \
  --access-token I7oAAAAAAABqRXQ5hghQl6Kc8xJtVmqcc5k5Skpnzg6jVKew \
  --cipher 89abcdefghjkmnpqrstvwxyz23456789abcdefghjkmnpqrstuvw \
  --basin my-opbox-basin 

# and finally, start syncing
ob start
```
