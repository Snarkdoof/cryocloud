import json
import os.path

ccmodule = {
    "description": "Read geojson and output the features as lists",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "GeoJSON file to read",
        "keys": "Which keys to deliver on. By default the whole feature is provided",
        "max_posts": "Maximum number of features to post (use to limit processing batches), default no limit",
        "shuffle": "Shuffle features before posting, default False",
        "dst": "Quick hack to check for existing files, should also have some kind of code to test...",
        "is_done": "Python method to check if done, use DST for destination dir, NAME for product name"
    },
    "outputs": {
        "features": "List of full feature"
    },
    "defaults": {
        "priority": 50,  # Normal
        "runOn": "success"
    },
    "status": {
        "progress": "Progress 0-100%",
        "state": "Current state of processing"
    }
}


def process_task(cc, task, stop_event):
    args = task["args"]
    if "src" not in args:
        raise Exception("Missing products source file")
    jsonfile = args["src"]

    if not os.path.exists(jsonfile):
        raise Exception("Missing JSON product file '%s'" % jsonfile)
    keys = args.get("keys", [])
    max_posts = int(args.get("max_posts", 0))
    shuffle = args.get("shuffle", False)

    dst = args.get("dst", None)
    is_done = args.get("is_done", None)

    if keys:
        ret = {k: [] for k in keys}
    else:
        ret = {"features": []}

    try:
        with open(jsonfile, "r") as f:
            features = json.load(f)
    except Exception as e:
        raise Exception("Can't read or parse JSON file '%s': %s" % (jsonfile, str(e)))

    if shuffle:
        import random
        random.shuffle(features)

    posted = 0
    skipped = 0
    for feature in features:
        if max_posts and posted >= max_posts:
            break
        posted += 1

        if is_done:
            # Do we already have the file?
            cmd = is_done.replace("$DST", dst).replace("$NAME", feature["name"])
            if eval(cmd):
                skipped += 1
                # cc.log.info("Skipping finished feature '%s'", feature["name"])
                continue

        if not keys:
            ret["features"].append(feature)
        else:
            for key in keys:
                if key == "features":
                    ret["features"].append(feature)
                elif key in feature:
                    ret[key].append(feature[key])
    cc.log.info("Processed %s items, skipped %d already done" % (posted, skipped))
    return 100, ret
