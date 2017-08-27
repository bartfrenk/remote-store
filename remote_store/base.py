import os
from contextlib import contextmanager
import gzip
import boto3
from botocore.exceptions import ClientError


class RemoteFile:
    """
    Class that represents a remote S3 object.
    """

    def __init__(self, store, obj):
        """Create a RemoteFile object.

        :param store: The store that holds this object.
        :param obj: The object data, as returned from the boto S3 commands.
        :returns: A RemoteFile object.
        """
        self._store = store
        self._etag = obj["ETag"]
        self.key = obj["Key"]
        self.size = obj["Size"]
        self.modified = obj["LastModified"]

    def __repr__(self):
        return "<{0.__class__.__name__}({0.key})>".format(self)

    @contextmanager
    def open(self, mode="r"):
        """Return a file handle to a locally cached copy of the object.

        :param mode: The mode in which to open the file.  Passed directly as a
            parameter to `open`.
        :returns: None
        """
        # pylint: disable=protected-access
        cache_path = self._cache_path()
        if not os.path.isfile(cache_path):
            self._download()
        h = gzip.open(cache_path, mode)
        yield h
        h.close()

    @property
    def is_cached(self):
        """Return whether there is a local copy of the file in the cache.

        :returns: Whether the object is cached.
        """
        return os.path.isfile(self._cache_path())

    def _download(self):
        # pylint: disable=protected-access
        self._store._download(self)
        return self

    def _cache_path(self):
        # pylint: disable=protected-access
        return self._store._cache_path(self)

    def clear_cached(self):
        """Remove the locally cached copy.

        :returns: None
        """
        if self.is_cached:
            os.remove(self._cache_path())


class RemoteStore:
    """Proxy for files stored in an S3 bucket."""

    def __init__(self, bucket, cache_dir="/tmp", creds=None, file_cls=RemoteFile):
        """Create a RemoteStore object.

        :param bucket: The underlying bucket.
        :param cache_dir: The path under which to store cached copies of the
            remote files in the bucket.
        :param creds: A dict of credentials passed as keyword arguments to the
            boto client constructor.
        :param file_cls: A data constructor mapping boto S3 objects to the
            output type of the `ls` method.  Provided for convenience, to simply
            return the S3 objects, pass in the identity function.  By default,
            :class:`~remote_store.RemoteFile`.

        :returns: A RemoteStore object.
        """
        self._bucket = bucket
        self._cache_dir = cache_dir + "/" + bucket
        self._file_cls = file_cls
        if creds is None:
            self._creds = {}
        else:
            self._creds = creds
        self._verbosity = 3
        self.__s3 = None

    def ls(self, prefixes=""):
        """Return iterator over store content matching one of multiple prefixes.

        :param prefixes: Either a single prefix, or a list of prefixes.  The
            order of the items yielded by the iterator corresponds to the order
            of the prefixes in the list.
        :returns: An iterator over objects of type `file_cls`.
        """
        if isinstance(prefixes, str):
            return self._ls(prefixes)
        return (self._ls(prefix) for prefix in prefixes)

    def _ls(self, prefix=""):
        """Return iterator over store content that matches prefix."""
        s3 = self._s3

        self._say(".")
        resp = s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)

        for obj in resp.get("Contents", []):
            yield obj

        while resp["IsTruncated"]:
            self._say(".")
            resp = s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix,
                                      ContinuationToken=resp["NextContinuationToken"])
            for obj in resp.get("Conents", []):
                yield self._file_cls(self, obj)

    @property
    def _s3(self):
        if self.__s3 is None:
            self.__s3 = boto3.client("s3", **self._creds)
        return self.__s3

    def _cache_path(self, rf):
        return self._cache_dir + "/" + rf.key

    def _download(self, rf):
        s3 = self._s3
        path = self._cache_path(rf)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(path, 'wb') as handle:
            self._say(".")
            try:
                s3.download_fileobj(self._bucket, rf.key, handle)
            except ClientError as e:
                print("Error downloading file: {} ({}, {})".format(e, e.args, e.response))
            return path

    def _say(self, msg, level=0):
        if level < self._verbosity:
            print(msg, end="", flush=True)

    def __repr__(self):
        return "<{0.__class__.__name__}(s3://{0._bucket})>".format(self)


# TODO: memoize this, taking into account expiration time
def assume_role(role, name):
    """Assumes an AWS role.  Simple wrapper around standard boto functionality.

    :param role: The ARN of the role to assume.
    :param name: The name of the session.

    :returns: A dict with keys `aws_access_key_id`, `aws_secret_access_key`, and
              `aws_session_token`.
    """
    sts = boto3.client("sts")
    resp = sts.assume_role(RoleArn=role, RoleSessionName=name)
    creds = resp["Credentials"]
    return {"aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"]}
