import csv
import cv2
from itertools import groupby
from operator import itemgetter
import pymongo
import argparse

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
database = mongo_client["Crucible"]
baselight_collection = database["Baselight"]
xytech_collection = database["Xytech"]


def args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baselight', type=argparse.FileType('r'), help="import baselight file",
                        required=False)
    parser.add_argument('--xytech', type=argparse.FileType('r'), help="import xytech file",
                        required=False)
    parser.add_argument('--process', type=argparse.FileType('r'), help="process video file",
                        required=False)
    parser.add_argument('--output', action='store_true', help="exports to xlsx",
                        required=False)
    return parser.parse_args()


def handle_args(result):
    if result.baselight is not None:
        import_baselight(result.baselight.name)

    if result.xytech is not None:
        import_xytech(result.xytech.name)

    if result.process is not None:
        process_video(result.process.name, result.output)


def import_baselight(file_path):
    with open(file_path) as f:
        for line in f:
            location_data = line.strip().split()
            location = location_data[0]
            frames = [int(frame) for frame in location_data[1:] if frame.isdigit()]
            data = {'location': location, 'frames': frames}
            baselight_collection.insert_one(data)


def import_xytech(file_path):
    work_order_number = None
    locations = []
    with open(file_path) as f:
        for line in f:
            if line.strip().startswith("Xytech Workorder"):
                work_order_number = line.split()[2]
            if line.strip().startswith("/"):
                locations.append(line)
        if work_order_number is not None:
            data = [{'work_order_number': work_order_number, 'location': location} for location in locations]
            xytech_collection.insert_many(data)


def process_video(file_path, output):
    clip = cv2.VideoCapture(file_path)
    fps = clip.get(cv2.CAP_PROP_FPS)
    frame_count = clip.get(cv2.CAP_PROP_FRAME_COUNT)
    print("Frame count: {}".format(frame_count))
    print("FPS: {}".format(fps))
    if output is not False:
        result = baselight_collection.find({"frames": {"$elemMatch": {"$gte": 0, "$lte": frame_count}}}, {'_id': 0})
        for data in result:
            print(data)
    print(get_time_code(fps, frame_count))


def get_time_code(fps, frame):
    total_seconds = frame / fps
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    frames = int((total_seconds - int(total_seconds)) * fps)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"


def export(xytech_file_path, baselight_file_path):
    with open("export.csv", 'w', newline='') as csv_file, open(xytech_file_path, 'r') as xytech_file, open(
            baselight_file_path, 'r') as baselight_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_NONE)
        writer.writerow(['Producer', 'Operator', 'Job', 'Notes'])
        locations = {}
        job_details = {h: '' for h in ['Producer', 'Operator', 'Job', 'Notes']}
        for line in xytech_file:
            if line.startswith('/'):
                parts = line.strip().split("/", 3)[1:]
                locations[parts[2]] = parts[:2]
            elif any(line.startswith(h) for h in job_details):
                key, value = line.strip().split(':', 1)
                job_details[key] = value.strip() if key != 'Notes' else next(xytech_file, '').strip()
        writer.writerows(
            [[job_details[h] for h in ['Producer', 'Operator', 'Job', 'Notes']], [], ["Location", "Frames to fix"]])
        for line in baselight_file:
            location = line.strip().split()
            location_key = location[0].split("/", 2)[2:][0]
            if location_key in locations:
                formatted_frames = []
                for k, g in groupby(
                        enumerate([item for item in location[1:] if '<err>' not in item and '<null>' not in item]),
                        lambda ix: int(ix[0]) - int(ix[1])):
                    frames = list(map(itemgetter(1), g))
                    formatted_frames.append(frames[0] + "-" + frames[len(frames) - 1] if len(frames) > 1 else frames[0])
                base_location = "/" + "/".join(locations[location_key][:2]) + "/" + location_key
                writer.writerows([[f"{base_location} {frame}"] for frame in formatted_frames])


if __name__ == '__main__':
    handle_args(args())
