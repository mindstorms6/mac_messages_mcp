"""One-shot HEIC preview conversion, isolated from the MCP process."""

import io
import sys
import warnings


def main():
    import pillow_heif
    from PIL import Image

    warnings.simplefilter("error", Image.DecompressionBombWarning)
    Image.MAX_IMAGE_PIXELS = 40_000_000
    pillow_heif.register_heif_opener()
    raw = sys.stdin.buffer.read(8_000_001)
    if len(raw) > 8_000_000:
        return 1
    with Image.open(io.BytesIO(raw)) as image:
        image.thumbnail((2048, 2048))
        image = image.convert("RGB")
        for _ in range(5):
            output = io.BytesIO()
            image.save(output, format="PNG")
            if output.tell() <= 3_000_000:
                sys.stdout.buffer.write(output.getvalue())
                return 0
            image.thumbnail(
                (max(1, image.width * 3 // 4), max(1, image.height * 3 // 4))
            )
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        # Never dump filenames or codec payloads into protocol output.
        raise SystemExit(1)
