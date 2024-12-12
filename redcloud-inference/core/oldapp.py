import base64
from io import BytesIO
from beam import endpoint, function, Image, Volume

# from model.dto import InferenceRequest, InferenceResponse
# from model.detector import Detector
# from model.db import DB
from utils.image_utils import generate_pil_image_from_base64

import snowflake.connector as sf
from inference import get_model
from inference.core.models.base import Model
import PIL.Image
import os

from typing import Any
from pydantic import BaseModel


class InferenceRequest(BaseModel):
    image: str  # base64 encoded image


class InferenceResponse(BaseModel):
    label: str
    confidence: float
    class_id: int
    # predictions: List[Dict[str, Any]]


API_KEY = os.environ["ROBOFLOW_API_KEY"]


class Detector:
    rb_model: Model

    def __init__(self):
        # load a pre-trained yolov8n model
        self.rb_model = get_model(model_id="access-pro-q4rmh/3", api_key=API_KEY)

    # define the image url to use for inference
    def analyse_image_roboflow(self, image: PIL.Image):

        # run inference on our chosen image, image can be a url, a numpy array, a PIL image, etc.
        results = self.rb_model.infer(image)[0]
        print(results.predictions)
        if len(results.predictions) == 0:
            return None
        return {
            "class_id": results.predictions[0].class_id,
            "label": results.predictions[0].class_name,
            "confidence": results.predictions[0].confidence,
        }
        # return {
        #     "class_id":results
        # }


def generate_pil_image_from_base64(base64_str: str) -> PIL.Image:
    """
    Generate a PIL image from a base64 encoded string
    :param base64_str: base64 encoded image
    """
    image = PIL.Image.open(BytesIO(base64.b64decode(base64_str)))
    return image


class DB:
    conn: sf.SnowflakeConnection

    def __init__(self):
        self.conn = sf.connect(
            account="cc88289.eu-west-1",
            user="joshua.eseigbe@redcloudtechnology.com",
            authenticator="externalbrowser",
            role="POC_RECOMMENDATION_USER_DEMO_GROUP",
            warehouse="DEV_ML",
            database="PRD_SANDBOX",
            schema="PUBLIC",
        )

    def run_query(self, query):
        """
        Run a query on the Snowflake database
        :param query: The query to run
        """
        cursor = self.conn.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()
            self.conn.close()


@endpoint(
    name="redcloud-inference",
    gpu="A10G",
    cpu=2,
    image=Image(
        python_packages="./requirements.txt",
        python_version="python3.11",
        commands=[
            "apt-get update -y",
            "apt-get install gcc g++ -y",
            "apt install libgl1 -y",
            "pip install opencv-python-headless",
        ],
    ),
    volumes=[
        Volume(name="data", mount_path="/data"),
        Volume(name="models", mount_path="/models"),
        Volume(name="checkpoints", mount_path="./checkpoints"),
    ],
)
def run_inference(**request):
    try:
        request_item = InferenceRequest.model_validate(request)
    except Exception as e:
        return {
            "message": str(e),
            "status": "error",
        }
    detector = Detector()

    image = generate_pil_image_from_base64(request_item.image)
    results = detector.analyse_image_roboflow(image)
    if results is None:
        return {"message": "No predictions found", "status": "error"}
    return {
        "message": "Inference successful",
        "status": "success",
        "result": results,
    }


if __name__ == "__main__":
    run_inference()
