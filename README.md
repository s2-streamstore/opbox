# opbox

*"Drop the 'dr-'. Just 'opbox'. It's cleaner."*

## installation

### from source

```bash
cargo install --path crates/client
cargo install --path crates/daemon
```

## use

### create a new workspace

```bash
export S2_ACCESS_TOKEN="my-access-token"
export S2_BUCKET_NAME="my-bucket-name"

cd /path/to/my/project

# start a new opbox workspace
ob init
```
This will print your `workspace_id`.

### clone an existing workspace

```bash
export S2_ACCESS_TOKEN="my-access-token"
export S2_BUCKET_NAME="my-bucket-name"

cd /path/to/my/project

# clone an existing opbox workspace
ob clone --workspace my-worspace-id-from-init
```

### sync!

```bash
cd /path/to/my/project
ob start
```