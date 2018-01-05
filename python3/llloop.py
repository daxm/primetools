#!/usr/bin/env python
"""
Automatic assignment handler for clLucas and manual testing at mersenne.org.
Should also work with CUDALucas and similar programs with minimal changes.
"""

__author__ = 'teknohog and daxm'

import os
import cookielib
import urllib2
import argparse
from .helper import network_getwork, fft_opt

primenet_baseurl = "http://www.mersenne.org/"
gpu72_baseurl = "http://www.gpu72.com/"


# Main Program.  (Put in "def main()"?)
parser = argparse.ArgumentParser()

parser.add_argument("-o", "--cllopts",
                    help="CLLucas options in a single string, e.g. '-d 0 -polite 0 -threads 128 -sixstepfft'")
parser.add_argument("-u", "--username", dest="username", required=True, help="Primenet user name")
parser.add_argument("-p", "--password", dest="password", required=True, help="Primenet password")
parser.add_argument("-n", "--num_cache", type=int, default=1,
                    help="Number of assignments to cache, default %(default)d")
# -t is reserved for timeout as in mfloop.py, although not currently used here
parser.add_argument("-T", "--worktype", dest="worktype", default="101",
                    help="Worktype code, default %(default)s for DC, alternatively 100 or 102 for first-time LL")
parser.add_argument("-w", "--workdir", dest="workdir", default=".",
                    help="Working directory with clLucas binary, default current")

options = parser.parse_args()
workdir = os.path.expanduser(options.workdir)

workfile = os.path.join(workdir, "worktodo.txt")

resultsfile = os.path.join(workdir, "results.txt")

# A cumulative backup
sentfile = os.path.join(workdir, "results_sent.txt")

workpattern = r"(DoubleCheck|Test)=.*(,[0-9]+){3}"

# mersenne.org limit is about 4 KB; stay on the safe side
sendlimit = 3500

# adapted from http://stackoverflow.com/questions/923296/keeping-a-session-in-python-while-making-http-requests
primenet_cj = cookielib.CookieJar()
primenet = urllib2.build_opener(urllib2.HTTPCookieProcessor(primenet_cj))

primenet_login = False

# Assuming clLucas in the workdir, could be generalized for any path
# and alternatives like CudaLucas...
binary = os.path.join(workdir, "clLucas")

while True:
    work = network_getwork()
    
    if len(work) == 0:
        print("Out of work")
        break
    else:
        worklist = [binary] + fft_opt(work) + options.cllopts.split() + [work]
        
    # Run clLucas in the foreground
    ecode = os.spawnvp(os.P_WAIT, worklist[0], worklist)
    
    if ecode != 0:
        print("Worker error")
        break
