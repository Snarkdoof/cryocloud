#!/usr/bin/env python3

import subprocess
import sys
import os.path

if len(sys.argv) != 3:
    raise SystemExit("Need to arguments, target md device and mount point")

md = sys.argv[1]
mount = sys.argv[2]

if not md.startswith("/dev"):
    md = "/dev/%s" % md

if not os.path.exists(mount):
    raise SystemExit("Missing mount point %s" % mount)

print("Creating %s and mounting it on %s" % (md, mount))

lsblk = subprocess.check_output("lsblk -d -o name".split(" ")).decode("utf-8").split(" ")

devices = []
for dev in lsblk:
    if dev.startswith("nvme"):
        devices.append(dev)

if len(devices) == 0:
    print("No nvme devices available")
    raise SystemExit(0)

print("  - found devices", devices)

mdadm = "sudo mdadm --create /dev/md0 --level=0 --raid-devices=%d" % len(devices)
mdadm = mdadm.split(" ")
mdadm.extend(["/dev/%s" % d for d in devices])


r = subprocess.call(mdadm)
if r != 0:
    print("FAILED")
else:
    print("md0 created")
    # Unmount if already mounted
    subprocess.call(["sudo", "umount", mount])

    # Format
    subprocess.call(["sudo", "mkfs.ext4", "-m", "0", md])
    r = subprocess.call(["sudo", "mount", md, mount])
    if r != 0:
        print("Mount failed")

raise SystemExit(r)
