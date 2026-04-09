"""Optional host bind mounts for Singularity/Docker job commands."""

from __future__ import annotations

import os

from dotenv import load_dotenv


def singularity_extra_bind_cli_fragments() -> tuple[str, str]:
    """Parse BWB_SINGULARITY_EXTRA_BINDS into singularity and docker CLI fragments.

    Format: semicolon-separated entries ``host_path:container_path`` or
    ``host_path:container_path:ro``. Whitespace around segments is ignored.
    Container paths must match paths used in workflow commands (e.g. STAR
    ``--genomeDir``) so that jobs see host data beyond the default ``/tmp``
    and ``/data`` workflow volumes.

    Returns:
        (singularity_extra, docker_extra) — each either empty or a leading
        space plus ``-B`` / ``-v`` flags.
    """
    load_dotenv()
    raw = (os.getenv("BWB_SINGULARITY_EXTRA_BINDS") or "").strip()
    if not raw:
        return "", ""
    sing_parts: list[str] = []
    docker_parts: list[str] = []
    for segment in raw.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        readonly = False
        if segment.endswith(":ro"):
            readonly = True
            segment = segment[:-3].rstrip()
        colon_idx = segment.find(":")
        if colon_idx < 0:
            continue
        host_src = segment[:colon_idx].strip()
        dest = segment[colon_idx + 1 :].strip()
        if not host_src or not dest:
            continue
        sing_frag = f"-B {host_src}:{dest}"
        dock_frag = f"-v {host_src}:{dest}"
        if readonly:
            sing_frag += ":ro"
            dock_frag += ":ro"
        sing_parts.append(sing_frag)
        docker_parts.append(dock_frag)
    sing = (" " + " ".join(sing_parts)) if sing_parts else ""
    dock = (" " + " ".join(docker_parts)) if docker_parts else ""
    return sing, dock
