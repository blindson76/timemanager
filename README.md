# Raft cluster demo

Edit `cluster.yaml` to configure member IDs, Raft and HTTP addresses, bootstrap
delay (seconds), and the number of members required for bootstrap.
The file is loaded from the current working directory.

Start each member in a separate terminal from this directory:

```sh
go run . --node-id node-1
go run . --node-id node-2
go run . --node-id node-3
```

The sample config also includes `node-4`. Only `--node-id` is accepted as a
configuration argument; its value must match a member in `cluster.yaml`.
