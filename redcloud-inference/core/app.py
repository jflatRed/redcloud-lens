import base64
from io import BytesIO
from beam import endpoint, Image, Volume
from utils.image_utils import generate_pil_image_from_base64
from ultralytics import YOLO
import PIL.Image
from pydantic import BaseModel


class InferenceRequest(BaseModel):
    image: str  # base64 encoded image


class InferenceResponse(BaseModel):
    label: str
    confidence: float
    class_id: int


class Detector:
    model: YOLO

    def __init__(self, model_path="/volumes/checkpoints/v2/train/weights/best.pt"):
        self.model = YOLO(model_path)
        print(self.model.names)
        self.default_confidence_score = 0.5

    # define the image url to use for inference
    def analyse_image(self, image: PIL.Image, conf=None):
        conf = conf if conf else self.default_confidence_score
        # run inference on our chosen image, image can be a url, a numpy array, a PIL image, etc.
        results = self.model.predict(image, conf)

        for result in results:
            if result.boxes:
                print(result)
                confidence = float(list(result.boxes.conf)[0])
                for x in result.boxes.cls:

                    class_id = int(x)
                    print(class_id)
                    break
                label = result.names[class_id]
                return {"confidence": confidence, "class_id": class_id, "label": label}

        return None


def generate_pil_image_from_base64(base64_str: str) -> PIL.Image:
    """
    Generate a PIL image from a base64 encoded string
    :param base64_str: base64 encoded image
    """
    image = PIL.Image.open(BytesIO(base64.b64decode(base64_str)))
    return image


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
    secrets=["ROBOFLOW_API_KEY"],
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
    results = detector.analyse_image(image)
    if results is None:
        return {"message": "No predictions found", "status": "error"}
    return {
        "message": "Inference successful",
        "status": "success",
        "result": results,
    }


if __name__ == "__main__":
    run_inference()
