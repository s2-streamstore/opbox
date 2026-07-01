# opbox

## about

> **⚠️ Warning:** Use this software with caution. Always keep additional backups of your data.

Real-time, editor agnostic, multiplayer sync for regular UTF-8 text files.

Great for collaborating on an Obsidian graph or a codebase without going through `git` for everything.

### how it works

A local daemon listens for both local and remote changes to files in a synced workspace directory.

For every text file in your directory, opbox locally maintains a CRDT-backed shadow version of it. When you save a change to a local file, it solves for an equivalent CRDT operation by doing a regular text diff, then modeling it as an update operation to the CRDT document.

This op is then published to other sync daemons by appending it to a shared, durable journal (S2).

Daemons listen for new ops on the journal, update the local CRDT shadow, and then materialize new versions of the text files on disk.

See [docs/architecture.md](docs/architecture.md) for more detail.

## running it

The only external service this relies on is [s2.dev](https://s2.dev). You can sign up for an account and use the cloud version, or run [s2-lite](https://github.com/s2-streamstore/s2#s2-lite) yourself.

> [!TIP]
> Head over to the [quickstart](docs/quickstart.md) to get up and running.

## architecture

See the in-progress [architecture notes](docs/architecture.md) and [design notes](docs/design-notes.md).

### spy

Monitor the shared log (CRDT operations being read from S2) in real time within a workspace:

```console
my-opbox-project % ob spy
spying on opbox workspace d5nev0w5bmzxrh44vsc6jd38rqhb03pb (pid 97662)
session  daemon=jc99r2
#4@1782939393173              169B  namespace   from=n20f2d(remote)  obj=ae1fxx  +claim="hello.txt" (text)
#5@1782939393173               13B  text        from=n20f2d(remote)  obj=ae1fxx  +0ch -0
#6@1782939397685               41B  text        from=n20f2d(remote)  obj=ae1fxx  +13ch -0  insert="hello world!\n"
#7@1782939411995               65B  namespace   from=n20f2d(remote)  obj=ae1fxx  -claim="hello.txt" (text)
#8@1782939448876              169B  namespace   from=jc99r2(you)  obj=fej65k  +claim="world.txt" (text)
#9@1782939448876               13B  text        from=jc99r2(you)  obj=fej65k  +0ch -0
#10@1782939458443              31B  text        from=n20f2d(remote)  obj=fej65k  +3ch -0  insert="yo\n"
#11@1782939468828              37B  text        from=jc99r2(you)  obj=fej65k  +9ch -0  insert="hi there "
```

### configuration

`ob config` manages typed opbox configuration. By default it writes user-wide defaults, such as the S2 access token and default basin:

```bash
ob config set access-token "MY_TOKEN"
ob config set default-basin "MY_BASIN"
```

You can override selected values for just the current workspace:

```bash
ob config --workspace set basin "WORKSPACE_BASIN"
ob config --workspace set daemon-log-level "opbox_core=trace,opbox_daemon=trace,info"
```

Workspace config lives in `.opbox/config.toml` and takes precedence over the user-wide config for that workspace. Restart the daemon after changing daemon settings:

```bash
ob stop && ob start

ob logs --follow
```
