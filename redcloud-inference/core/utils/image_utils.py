import base64
from io import BytesIO
from PIL import Image


def generate_pil_image_from_base64(base64_str: str) -> Image:
    """
    Generate a PIL image from a base64 encoded string
    :param base64_str: base64 encoded image
    """
    image = Image.open(BytesIO(base64.b64decode(base64_str)))
    return image
