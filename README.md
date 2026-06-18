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

You can override selected daemon environment values by creating a `.opbox/env` file in your workspace root. The file is read by the daemon at startup and is never written by `ob`.

This is also useful for increasing the log level of the daemon:

```bash
# in workspace root
echo "RUST_LOG=opbox_core=trace,opbox_daemon=trace,info" >> .opbox/env

# restart the daemon
ob stop && ob start

# tail the log file
ob logs --follow
```
