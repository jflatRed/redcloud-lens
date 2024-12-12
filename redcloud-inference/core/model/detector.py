from inference import get_model
from inference.core.models.base import Model
import PIL.Image
import os

API_KEY = os.environ["ROBOFLOW_API_KEY"]


class Detector:
    rb_model: Model

    def __init__(self):
        # load a pre-trained yolov8n model
        self.rb_model = get_model(model_id="access-pro-q4rmh/3", api_key=API_KEY)

        pass

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


if __name__ == "__main__":
    detector = Detector()
    image = PIL.Image.open(
        "redcloud-inference/core/istockphoto-499208007-1024x1024.jpg"
    )
    value = detector.analyse_image_roboflow(image)
    print(value)
    pass
