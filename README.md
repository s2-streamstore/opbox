# opbox

> Drop the "dr-". Just "opbox". It's cleaner...

## about

> **⚠️ Warning:** Use this software with caution. Always keep additional backups of your data.
> 
Real-time, multiplayer sync for plain text files on disk using CRDTs.

## running it

The only external service this relies on is [s2.dev](https://s2.dev). You can sign up for an account and use the cloud version, or run [s2-lite](https://github.com/s2-streamstore/s2#s2-lite) yourself.

> [!TIP] 
> Head over to the [quickstart](docs/quickstart.md) to get up and running.

### installation

#### from source

```bash
cargo install --path crates/client
cargo install --path crates/daemon
```

Then you interact with the `ob` command.

### use

#### configure

```bash
ob config set default-basin "opbox-dev"
ob config set access-token "-- my actual access token! --"
```

#### create a new workspace

```bash
export S2_ACCESS_TOKEN="my-access-token"
export S2_BASIN_NAME="my-basin-name"

cd /path/to/my/project

# start a new opbox workspace
ob init
```
This will print your `workspace_id`.

#### clone an existing workspace

```bash
export S2_ACCESS_TOKEN="my-access-token"
export S2_BASIN_NAME="my-basin-name"

cd /path/to/my/project

# clone an existing opbox workspace
ob clone --workspace my-worspace-id-from-init
```

#### sync!

```bash
cd /path/to/my/project
ob start
```

#### spy

Monitor the shared log (CRDT operations being read from S2) in real time within a workspace:

```console
my-opbox-project % ob spy
spying on opbox workspace e4wtker801s559vp97drk6xbnfkq6ez7 (pid 41152)
#92      text        obj=IUO8u30p  from=n2c4dr0x  outbox=89  35B  ts=1781201298256000000  +4ch -0  insert=" ok\n"
#93      text        obj=tVFw/DK6  from=n2c4dr0x  outbox=90  1422B  ts=1781201298297000000  +1180ch -74  insert="&amp;amp;amp;quot;\n&amp;amp;amp;quot;\n&amp;amp;amp;quot;\n&amp;amp;amp;quot;\n&amp;amp;amp;quot;\n&amp;amp;amp;quot;\n..."
#94      text        obj=IUO8u30p  from=n2c4dr0x  outbox=91  46B  ts=1781201303180000000  +15ch -0  insert="I dont know why"
#95      text        obj=IUO8u30p  from=n2c4dr0x  outbox=92  37B  ts=1781201307152000000  +6ch -0  insert="otest\n"
#96      text        obj=IUO8u30p  from=n2c4dr0x  outbox=93  27B  ts=1781201309542000000  +0ch -30
#97      text        obj=WUmyY6V/  from=n2c4dr0x  outbox=94  29B  ts=1781201316564000000  +1ch -0  insert="\n"
#98      text        obj=WUmyY6V/  from=n2c4dr0x  outbox=95  33B  ts=1781201317024000000  +5ch -0  insert="yo yo"
#99      text        obj=WUmyY6V/  from=n2c4dr0x  outbox=96  24B  ts=1781201322218000000  +0ch -6
```

### configuration

Can override env vars for the daemon by creating a `.opbox/env` file in your workspace root.

This is also useful for increasing the log level of the daemon:

```bash
# in workspace root
echo "RUST_LOG=opbox=trace,info" >> .opbox/env

# restart the daemon
ob stop && ob start

# tail the log file
ob tail -f # (just a wrapper over `tail -f ./opbox/daemon.log`)
```