# RemoteStore

## Set up

Generate documentation (requires Sphinx with access to the ReadTheDocs style):

```bash
make docs
```

## Sample usage


```python
from remote_store import assume_role, RemoteStore

creds = assume_role("arn:xxx", "session_name")
store = RemoteStore("s3://some_bucket, creds=creds)
it = store.ls(["/prefix1", "/prefix2"])

with next(it).open("r") as h:
    for line in h:
        print(line)
```


