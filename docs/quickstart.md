# Quickstart

## Prerequisites

### Install opbox

#### from source

```bash
cargo install --path crates/client
cargo install --path crates/daemon
```

You should have `ob` and `opbox-daemon` in your `$PATH` now.

#### from release

TODO

### S2 configuration

#### Using `s2.dev`

Go to [s2.dev](http://s2.dev) and make an account. You can sign on with SSO and get started immediately. All new signups get $10 of credits, which is way more than enough for any reasonable `opbox` workspace.

> [!NOTE]
> Do put down a payment method in order to be able to make streams with infinite retention. The free tier (no payment method listed) is restricted to 28 day data retention. In other words, your workspace will break after 4 weeks unless you do this.

Create an access token on the UI, and hold on to it.

Next, create a basin, making sure that:
- You enable automatic stream creation `on append`
- The retention policy is set to `Infinite` (age-based will work for short-lived workspaces)

![create_basin.png](images/create_basin.png)

Configure your local opbox using your access token and basin name:

```bash
ob config set access-token "MY_TOKEN"
ob config set default-basin "MY_BASIN"
```

This configuration is stored in an OS user-level config file, and will be used for all opbox workspaces unless the workspace has a local override.

At this point, you're set.

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
  basin          opbox-dev
  root           /Users/me/my-opbox-workspace

your workspace is: tgyz0q5a5051djmmpsm6vy7fv3m3egy4

run ob start to begin syncing
```

Great, it worked.

At this point, the workspace has been created, and an initial snapshot has been successfully sent to S2.

Anyone who wants to sync can clone this workspace (as long as they have a valid auth token, and also know your basin).

To listen for local changes and apply remote changes, start the daemon:
```bash
ob start
```

## Cloning an existing workspace

> [!NOTE] 
> Make sure your opbox config is correct.
> If you did the S2 setup steps, just make sure to send the access token and basin to anyone you want to share your workspace with. They will also need to configure via `ob config`.

This will likely be done on another computer.

```bash
mkdir -p ~/my-opbox-workspace-clone-1
cd ~/my-opbox-workspace-clone-1

# the directory must be empty to start
# then, use the workspace id from earlier
ob clone --workspace tgyz0q5a5051djmmpsm6vy7fv3m3egy4 
```





