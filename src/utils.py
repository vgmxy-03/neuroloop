import atexit
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional


_LSL_CFG_PATH: Optional[str] = None


def get_utc_timestamp() -> str:
    """
    Get current UTC timestamp in ISO 8601 format.

    Returns:
    --------
    str : ISO 8601 formatted timestamp with UTC timezone
    """
    return datetime.now(timezone.utc).isoformat()


def configure_lsl_api_cfg() -> None:
    """Configure liblsl via a temporary config file to reduce console verbosity."""
    global _LSL_CFG_PATH

    if "LSLAPICFG" in os.environ:
        return  # Already configured externally

    if _LSL_CFG_PATH is not None:
        os.environ["LSLAPICFG"] = _LSL_CFG_PATH
        return

    cfg_fd, cfg_path = tempfile.mkstemp(prefix="lsl_api_", suffix=".cfg")
    try:
        with os.fdopen(cfg_fd, "w", encoding="ascii") as fh:
            fh.write(
                """
[ports]
IPv6 = disable

[log]
level = -1
""".lstrip()
            )
    except Exception:
        # If writing fails, close and remove the file and continue without config
        try:
            os.close(cfg_fd)
        except Exception:
            pass
        try:
            os.remove(cfg_path)
        except Exception:
            pass
        return

    os.environ["LSLAPICFG"] = cfg_path
    _LSL_CFG_PATH = cfg_path

    def _cleanup_cfg() -> None:
        try:
            os.remove(cfg_path)
        except Exception:
            pass

    atexit.register(_cleanup_cfg)
