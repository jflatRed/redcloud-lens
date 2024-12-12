# from ultralytics.data.converter import convert_coco

from collections import defaultdict
import json
import os
import numpy as np
from ultralytics.data.converter import (
    coco91_to_coco80_class,
    increment_path,
    TQDM,
    merge_multi_segment,
    LOGGER,
)

from pathlib import Path


def convert_coco(
    labels_dir="../coco/annotations/",
    save_dir="coco_converted/",
    use_segments=False,
    use_keypoints=False,
    cls91to80=True,
    lvis=False,
):
    """
    Converts COCO dataset annotations to a YOLO annotation format  suitable for training YOLO models.

    Args:
        labels_dir (str, optional): Path to directory containing COCO dataset annotation files.
        save_dir (str, optional): Path to directory to save results to.
        use_segments (bool, optional): Whether to include segmentation masks in the output.
        use_keypoints (bool, optional): Whether to include keypoint annotations in the output.
        cls91to80 (bool, optional): Whether to map 91 COCO class IDs to the corresponding 80 COCO class IDs.
        lvis (bool, optional): Whether to convert data in lvis dataset way.

    Example:
        ```python
        from ultralytics.data.converter import convert_coco

        convert_coco("../datasets/coco/annotations/", use_segments=True, use_keypoints=False, cls91to80=True)
        convert_coco("../datasets/lvis/annotations/", use_segments=True, use_keypoints=False, cls91to80=False, lvis=True)
        ```

    Output:
        Generates output files in the specified output directory.
    """
    # Create dataset directory
    save_dir = increment_path(save_dir)  # increment if save directory already exists
    for p in save_dir / "labels", save_dir / "images":
        p.mkdir(parents=True, exist_ok=True)  # make dir
    # Convert classes
    coco80 = coco91_to_coco80_class()

    # Import json
    for json_file in sorted(Path(labels_dir).resolve().glob("*.json")):
        lname = "" if lvis else json_file.stem.replace("instances_", "")
        fn = Path(save_dir) / "labels" / lname  # folder name
        fn.mkdir(parents=True, exist_ok=True)
        if lvis:
            # NOTE: create folders for both train and val in advance,
            # since LVIS val set contains images from COCO 2017 train in addition to the COCO 2017 val split.
            (fn / "train2017").mkdir(parents=True, exist_ok=True)
            (fn / "val2017").mkdir(parents=True, exist_ok=True)
        with open(json_file) as f:
            data = json.load(f)
            print(data)

        # Create image dict
        images = {f'{x["id"]:d}': x for x in data["images"]}
        # Create image-annotations dict
        imgToAnns = defaultdict(list)
        for ann in data["annotations"]:
            imgToAnns[ann["image_id"]].append(ann)

        image_txt = []
        count = 0
        # Write labels file
        for img_id, anns in TQDM(imgToAnns.items(), desc=f"Annotations {json_file}"):
            img = images[f"{img_id:d}"]
            h, w = img["height"], img["width"]
            f = (
                str(Path(img["coco_url"]).relative_to("http://images.cocodataset.org"))
                if lvis
                else img["file_name"]
            )
            if lvis:
                image_txt.append(str(Path("./images") / f))

            bboxes = []
            segments = []
            keypoints = []
            for ann in anns:
                if ann.get("iscrowd", False):
                    continue
                # The COCO box format is [top left x, top left y, width, height]
                box = np.array(ann["bbox"], dtype=np.float64)
                box[:2] += box[2:] / 2  # xy top-left corner to center
                box[[0, 2]] /= w  # normalize x
                box[[1, 3]] /= h  # normalize y
                if box[2] <= 0 or box[3] <= 0:  # if w <= 0 and h <= 0
                    continue

                cls = (
                    coco80[ann["category_id"] - 1]
                    if cls91to80
                    else ann["category_id"] - 1
                )  # class
                box = [cls] + box.tolist()
                if box not in bboxes:
                    bboxes.append(box)
                    if use_segments and ann.get("segmentation") is not None:
                        if len(ann["segmentation"]) == 0:
                            segments.append([])
                            continue
                        elif len(ann["segmentation"]) > 1:
                            s = merge_multi_segment(ann["segmentation"])
                            s = (
                                (np.concatenate(s, axis=0) / np.array([w, h]))
                                .reshape(-1)
                                .tolist()
                            )
                        else:
                            s = [
                                j for i in ann["segmentation"] for j in i
                            ]  # all segments concatenated
                            s = (
                                (np.array(s).reshape(-1, 2) / np.array([w, h]))
                                .reshape(-1)
                                .tolist()
                            )
                        s = [cls] + s
                        segments.append(s)
                    if use_keypoints and ann.get("keypoints") is not None:
                        keypoints.append(
                            box
                            + (
                                np.array(ann["keypoints"]).reshape(-1, 3)
                                / np.array([w, h, 1])
                            )
                            .reshape(-1)
                            .tolist()
                        )

            # Write
            if len(bboxes):
                # Define the path where the file is being written
                file_path = (fn / f).with_suffix(".txt")

                # Open file for appending
                with open(file_path, "a") as file:
                    written_data = False  # Flag to track if any data is written

                    for i in range(len(bboxes)):
                        if use_keypoints:
                            line = (*(keypoints[i]),)  # cls, box, keypoints
                        else:
                            line = (
                                *(
                                    segments[i]
                                    if use_segments and len(segments[i]) > 0
                                    else bboxes[i]
                                ),
                            )  # cls, box or segments

                        # Try to write to the file
                        try:
                            file.write(("%g " * len(line)).rstrip() % line + "\n")
                            written_data = True  # Mark that data was written
                        except:
                            count += 1
                            print(count)

                # If no data was written, delete the file
                if not written_data:
                    os.remove(file_path)
                    print(f"File {file_path} was empty and has been deleted.")

        if lvis:
            with open(
                (
                    Path(save_dir)
                    / json_file.name.replace("lvis_v1_", "").replace(".json", ".txt")
                ),
                "a",
            ) as f:
                f.writelines(f"{line}\n" for line in image_txt)

    LOGGER.info(
        f"{'LVIS' if lvis else 'COCO'} data converted successfully.\nResults saved to {save_dir.resolve()}"
    )


convert_coco(
    "./datasets", "./datasets", use_segments=True, use_keypoints=False, cls91to80=True
)
# convert_coco("../datasets/lvis/annotations/", use_segments=True, use_keypoints=False, cls91to80=False, lvis=True)
