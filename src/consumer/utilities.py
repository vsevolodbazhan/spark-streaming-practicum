from pathlib import Path

from s3path import S3Path


def convert_path_to_string(path: Path | S3Path) -> str:
    """Convert Path to string, using s3a:// protocol for S3 paths."""
    if isinstance(path, S3Path):
        return path.as_uri().replace("s3://", "s3a://")
    return path.as_posix()
