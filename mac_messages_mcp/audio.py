"""Local, bounded voice-message conversion for MCP AudioContent."""

import subprocess
import tempfile
from pathlib import Path

MAX_AUDIO_SOURCE_BYTES = 8_000_000
MAX_AUDIO_INLINE_BYTES = 3_000_000
_AUDIO_EXTENSIONS = {
    ".caf",
    ".mp3",
    ".m4a",
    ".aac",
    ".wav",
    ".ogg",
    ".opus",
    ".flac",
    ".aif",
    ".aiff",
    ".amr",
}


def is_audio_attachment(path, mime, uti):
    return (
        mime.startswith("audio/")
        or uti == "com.apple.coreaudio-format"
        or (not mime or mime == "application/octet-stream")
        and Path(path).suffix.lower() in _AUDIO_EXTENSIONS
    )


def audio_to_mp3(raw):
    """Return complete MP3 bytes or an explicit failure, never a partial clip.

    A seekable temporary input is needed for CAF's trailing packet table.
    Restrict demuxers to audio containers and disable network protocols.
    """
    if not raw or len(raw) > MAX_AUDIO_SOURCE_BYTES:
        return None
    try:
        with tempfile.TemporaryDirectory(prefix="messages-audio-") as directory:
            source = Path(directory) / "input"
            source.write_bytes(raw)
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-nostdin",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-protocol_whitelist",
                    "file,pipe",
                    "-format_whitelist",
                    "caf,wav,mp3,mov,ogg,flac,aac,aiff,amr",
                    "-i",
                    str(source),
                    "-map",
                    "0:a:0",
                    "-vn",
                    "-map_metadata",
                    "-1",
                    "-ac",
                    "1",
                    "-ar",
                    "24000",
                    "-c:a",
                    "libmp3lame",
                    "-b:a",
                    "64k",
                    "-threads",
                    "1",
                    "-fs",
                    str(MAX_AUDIO_INLINE_BYTES + 1),
                    "-f",
                    "mp3",
                    "pipe:1",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=20,
                check=False,
            )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0 or not 0 < len(result.stdout) <= MAX_AUDIO_INLINE_BYTES:
        return None
    return result.stdout
