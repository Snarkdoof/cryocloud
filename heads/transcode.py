
from __future__ import print_function
import CryoCloud
import os.path


description = """
Transcode files using FFMPEG
"""


# We have some additional arguments we'd like
def addArguments(parser):
    print("Adding arguments")
    parser.add_argument("-b", "--bitrate", dest="bitrate_video",
                        default=None,
                        help="Bitrate for video")

    parser.add_argument("--format", dest="video_format",
                        default="webm",
                        help="Video format")

    parser.add_argument("-s", "--size", dest="size",
                        default=None,
                        help="Video size")


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, options):

        self._tasks = []
        self._taskid = 1

        self.options = options

        print("Monitoring directory", self.options.input_dir)
        self.dir_monitor = self.head.makeDirectoryWatcher(self.options.input_dir, self.onAdd,
                                                          recursive=self.options.recursive)
        self.dir_monitor.start()

        if options.force:
            print("Resetting all files")
            self.dir_monitor.reset()

    def onAdd(self, info):
        fdir, fname = os.path.split(info["fullpath"])
        print("Found file for processing", info["relpath"])

        task = {
            "src": info["fullpath"],
            "size": self.options.size,
            "bitrate_video": self.options.bitrate_video
        }

        # Output

        task["dst"] = os.path.join(self.options.output_dir,
                                   os.path.splitext(fname)[0] + "." + self.options.video_format)
        if os.path.exists(task["dst"]):
            self.head.log.warning("Path already exists: %s" % task["dst"])
            print("Path already exists", task["dst"])
            return

        self._tasks.append(self._taskid)
        self.head.add_job(1, self._taskid, task, module="ffmpeg")
        self._taskid += 1

    def onAllocated(self, task):
        print("Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"])

    def onCompleted(self, task):
        print("Task completed:", task["taskid"], task["step"])
        self._tasks.remove(task["taskid"])
        print("Setting done", task["args"]["src"])
        self.dir_monitor.setDone(task["args"]["src"])

        print(len(self._tasks), "tasks left")
        if len(self._tasks) == 0:
            print("DONE")
            self.head.stop()

    def onTimeout(self, task):
        print("Task timeout out, requeue")
        self.head.requeue(task)

    def onError(self, task):
        print("Error for task", task)
        # Notify someone, log the file as failed, try to requeue it or try to figure out what is wrong and how to fix it
        pass

    def onStepCompleted(self, step):
        print("All OK Step", step, " - waiting for more files")
        # self.head.stop()
