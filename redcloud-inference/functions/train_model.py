from beam import function, Volume, Image
from ultralytics import YOLO, settings, SETTINGS
import requests
import os


def download_file(url, save_path):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Download the file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"File saved to {save_path}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


@function(
    name="train-model",
    gpu="A10G",
    cpu=4,
    memory="32Gi",
    image=Image(
        python_packages=["ultralytics", "requests"],
        python_version="python3.11",
        commands=[
            "apt-get update -y",
            "apt-get install gcc g++ jq -y",  # jq is used to parse and modify JSON
            "apt install libgl1 -y",
            "pip install opencv-python-headless",
            "yolo settings",
            "yolo settings datasets_dir=./data/dataset",
            "yolo settings",
        ],
    ),
    volumes=[
        Volume(name="data", mount_path="/data"),
        Volume(name="checkpoints", mount_path="./checkpoints"),
    ],
)
def train_model():
    # Load a model

    pt_path = (
        "https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt"
    )
    pt_save_path = "./data/yolo11/yolo11n.pt"
    download_file(pt_path, pt_save_path)
    SETTINGS.update(
        {
            "datasets_dir": "./data/dataset",
        }
    )
    print(settings)
    model = YOLO(pt_save_path)
    # model = YOLO(pt_save_path)  # load a pretrained model (recommended for training)
    # Train the model
    results = model.train(
        data="config.yaml",
        project="./checkpoints/v2",
        epochs=100,
        imgsz=640,
    )


if __name__ == "__main__":
    train_model()
