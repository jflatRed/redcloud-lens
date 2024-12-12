import os
import shutil
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from beam import Volume, Image, function

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
region = os.environ["REGION"]


class ProofUpload:
    def __init__(
        self,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region=region,
    ):
        self.s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def delete_folder(self, folder_path):
        try:
            shutil.rmtree(folder_path)  # Removes the folder and its contents
            print(f"Folder '{folder_path}' and its contents have been deleted.")
        except Exception as e:
            print(f"Error: {e}")

    def upload(self, **inputs):
        source_dir = inputs["local_path"]
        destination_dir = inputs["beam_volume_path"]

        def copy_directory(source_dir, destination_dir):
            try:
                # Create the destination directory if it doesn't exist
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)

                # Iterate over all files and subdirectories in the source directory
                for item in os.listdir(source_dir):
                    source_item = os.path.join(source_dir, item)
                    destination_item = os.path.join(destination_dir, item)

                    # If it's a directory, recursively copy it
                    if os.path.isdir(source_item):
                        copy_directory(source_item, destination_item)
                    else:
                        # If it's a file, copy it
                        shutil.copy2(
                            source_item, destination_item
                        )  # Use copy2 to preserve metadata

                print(f"Successfully copied '{source_dir}' to '{destination_dir}'")

            except Exception as e:
                print(f"Error copying '{source_dir}' to '{destination_dir}': {str(e)}")

        copy_directory(source_dir=source_dir, destination_dir=destination_dir)

    def process_dataset(**inputs):
        dirs = inputs["local_paths"]
        # Create the destination directory if it doesn't exist
        for path in dirs:
            print(f"Creating directory: {path}={os.path.exists(path)}")

            if not os.path.exists(path):
                try:
                    print("triggered")
                    os.makedirs(path)
                    file_name = "new_file.txt"
                    file_path = os.path.join(path, file_name)

                    # Create the file
                    with open(file_path, "w") as file:
                        file.write("This is the content of the new file.")

                except Exception as e:
                    print(f"Error creating '{path}': {str(e)}")

    def process_images(
        self,
        destination_dir="./data/urine/pr-urine/test",
        key="urine/Urine test-pr-pilot",
    ):
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        # use pagination to get all the objects in the bucket
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket="red-cloud", Prefix=key)
        for page in pages:
            print(len(page["Contents"]))
            for obj in page["Contents"]:
                local_file_path = os.path.join(
                    destination_dir,
                    obj["Key"].split("/")[-1],
                )
                print(local_file_path)
                try:
                    self.s3_client.download_file(
                        "red-cloud", obj["Key"], local_file_path
                    )
                except Exception as e:
                    print(e)

    def upload_to_s3(self, file_path, key, bucket="proof_detection"):
        # file_path = "./out/47041731.mp4"
        upload = self.s3_client.upload_file(file_path, bucket, key)
        print(upload)
        return upload

    def download_s3_file(self, bucket_name, key, download_path):
        try:
            # Download the file from the specified bucket and key
            self.s3_client.download_file(bucket_name, key, download_path)
            print(
                f"Successfully downloaded {key} from bucket {bucket_name} to {download_path}"
            )
            return download_path
        except FileNotFoundError:
            print(f"The file {download_path} was not found.")
        except NoCredentialsError:
            print("Credentials not available.")
        except PartialCredentialsError:
            print("Incomplete credentials provided.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def download_s3_folder(
        self,
        local_dir,
        s3_folder,
        bucket_name="red-cloud",
    ):
        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 folder
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_folder)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Get the full path of the object
                    s3_key = obj["Key"]
                    # Remove the folder prefix from the key to get the relative path
                    relative_path = os.path.relpath(s3_key, s3_folder)
                    # Create the local file path
                    local_file_path = os.path.join(local_dir, relative_path)

                    # Ensure the local directory exists
                    local_file_dir = os.path.dirname(local_file_path)
                    if not os.path.exists(local_file_dir):
                        os.makedirs(local_file_dir)

                    # Download the file
                    self.s3_client.download_file(bucket_name, s3_key, local_file_path)
                    print(f"Downloaded {s3_key} to {local_file_path}")


image_destination_dir = "./data/dataset/test/images"
destination_dir = "./data/dataset/valid/labels"


@function(
    volumes=[
        Volume(name="data", mount_path="./data"),
        Volume(name="checkpoints", mount_path="./checkpoints"),
        # weights,
    ],
    cpu=2,
    memory="10Gi",
    name="upload",
    image=Image(commands=["pip install boto3"]),
    secrets=[
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "REGION",
    ],
)
def upload_images():
    print("Uploading images...")
    upload_client = ProofUpload()
    upload_client.process_images(
        destination_dir=image_destination_dir,
        key="",
    )


@function(
    volumes=[
        Volume(name="data", mount_path="./data"),
        Volume(name="checkpoints", mount_path="./checkpoints"),
        # weights,
    ],
    cpu=2,
    memory="10Gi",
    name="upload",
    image=Image(commands=["pip install boto3"]),
    secrets=[
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "REGION",
    ],
)
def upload_dataset():
    import os

    upload_client = ProofUpload()

    # Path to the directory
    path = "./datasets2/labels/_annotations.coco"

    # List all items in the directory
    items = os.listdir(path)
    # for item in items:
    #     local_path = "./datasets2/labels/_annotations.coco" + "/" + item
    upload_client.upload(
        local_path=path,
        beam_volume_path=destination_dir,
    )


@function(
    volumes=[
        Volume(name="data", mount_path="./data"),
        Volume(name="checkpoints", mount_path="./checkpoints"),
        # weights,
    ],
    cpu=2,
    memory="10Gi",
    name="upload",
    image=Image(commands=["pip install boto3"]),
    secrets=[
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "REGION",
    ],
)
def delete_folder():
    to_delete = [
        "./data/dataset/train/labels",
        "./data/dataset/test/labels",
        "./data/dataset/valid/labels",
    ]
    client = ProofUpload()
    for x in to_delete:
        client.delete_folder(x)


#  beam run s3_data_store.py:upload_to_s3
if __name__ == "__main__":
    upload_dataset()
    # ProofUpload().process_images("./data/hands/hands-1", "hands_data")
