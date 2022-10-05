import subprocess
import fcntl
import os
import select
import re

import CryoCore

ccmodule = {
    "description": "Encode/Transcode using FFMPEG",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Source",
        "dst": "Destination",
        "size": "Size of finished video (WxH)",
        "bitrate_video": "Bitrate of final video (1200k)",
        "acopy": "Copy audio track",
        "vcopy": "Copy video track",
        "copy": "Copy both audio and video track",
        "noaudio": "Drop the audio track",
        "mono": "Convert to mono",
        "audio_hz": "Rate - default keep, use 16000 for 16khz",
        "container": "Specify the container, 'auto' will guess one, blank will let ffmpeg handle it"
    },
    "outputs": {
        "size": "Final size in MB",
        "time": "Time used for operation",
        "dst": "Output path"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "run": "success"
    }
}


def canrun():
    """
    Check if we are allowed to run on this machine
    """
    try:
        return subprocess.check_call(["ffmpeg", "-version"], sdtout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
    except:
        return False


def process_task(worker, task):
    args = task["args"]
    if "src" not in args:
        raise Exception("Missing src")

    if "dst" not in args:
        raise Exception("Missing dst")

    dst = args["dst"]
    cmd = ["ffmpeg", "-y", "-i", args["src"]]
    if "size" in args:
        cmd.extend(["-s", args["size"]])
    if "bitrate_video" in args:
        cmd.extend(["-b:v", args["bitrate_video"]])

    if "acopy" in args:
        cmd.extend(["-c:a", "copy"])
    if "vcopy" in args:
        cmd.extend(["-c:v", "copy"])
    if "copy" in args:
        cmd.extend(["-c:a", "copy", "-c:v", "copy"])
    if "noaudio" in args:
        cmd.extend(["-na"])
    if "mono" in args:
        cmd.extend(["-ac", "1"])
    if "audio_hz" in args:
        cmd.extend(["-ar", str(args["audio_h"])])

    if "container" in args:
        if args["container"] == "auto":
            worker.status["state"] = "probing"
            # Probe the file to see what it is (source likely doesn't have an extension)
            probe = "ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1".split(" ")
            probe.append(args["src"])
            output = subprocess.check_output(probe).decode("utf-8").split("\n")[0]
            print("Probe found container to be", output)
            ext = {"h264": "mp4", "h265": "mp4", "vp8": "webm", "vp9": "webm"}
            if output not in ext:
                raise Exception("Unsupported container: %s, known codecs are %s" % (output, list(ext)))
            dst += "." + ext[output]
        else:
            dst += "." + args["container"]

    cmd.append(dst)
    worker.status["state"] = "converting"
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # We set the outputs as nonblocking
    fcntl.fcntl(p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

    items = ["frame", "fps", "q", "size", "time", "bitrate", "speed"]
    for i in items:
        worker.status.new(i, expire_time=600)
        worker.status[i].downsample(cooldown=5)

    # read buffers
    buf = {p.stdout: "", p.stderr: ""}

    while not CryoCore.API.api_stop_event.is_set():
        ready_fds = select.select([p.stdout, p.stderr], [], [], 1.0)[0]
        for fd in ready_fds:
            data = fd.read()
            buf[fd] += data.decode("utf-8")

        # process stdout - line by line
        while buf[p.stdout].find("\n") > -1:
            line, buf[p.stdout] = buf[p.stdout].split("\n", 1)
            print("stdout", line)

        # process stderr
        data = buf[p.stderr]
        if data:
            buf[p.stderr] = ""
            m = re.match("frame=\s*(\d+) fps=\s*(\S*) q=(\S*) size=\s*(\d+\S*) time=(\S*) bitrate=\s*(\S+) speed=\s*(\S+)x", data)
            if m:
                for i in range(0, len(items)):
                    worker.status[items[i]] = m.groups()[i]

        # check if the process is still running
        _retval = p.poll()
        if _retval is not None:
            # Process exited
            if _retval == 0:
                break
            # unexpected
            _err = "Wrapped process '%s' exited with value %d" % (cmd, _retval)
            worker.status["state"] = "failed"
            raise Exception(_err)
            break

    worker.status["state"] = "idle"

    return 100, {"size": worker.status["size"].get_value(), "time": worker.status["time"].get_value(), "dst": dst}
