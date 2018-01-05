#!/usr/bin/env python
"""
Automatic assignment handler for manual testing at mersenne.org and optionally gpu72.com.

Written with mfakto in mind, this only handles trial factoring work for now. It should work with mfaktc as well.

This version can run in parallel with the factoring program, as it uses lockfiles to avoid conflicts when updating files.
"""

__author__ = 'teknohog and daxm'

import sys
import os.path
import cookielib
import urllib2
import re
from time import sleep
import os
import urllib
import math
from optparse import OptionParser
from .helper import submit_work, debug_print, get_assignment

primenet_baseurl = "https://www.mersenne.org/"
gpu72_baseurl = "https://www.gpu72.com/"



# Main program
parser = OptionParser()

parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="Display debugging info")

parser.add_option("-e", "--exp", dest="max_exp", default="72", help="Upper limit of exponent, default 72")

parser.add_option("-T", "--gpu72type", dest="gpu72_type", default="lltf",
                  help="GPU72 type of work, lltf or dctf, default lltf.")

parser.add_option("-o", "--gpu72option", dest="gpu72_option", default="what_makes_sense",
                  help="GPU72 Option to fetch, default what_makes_sense. Other valid values are lowest_tf_level,"
                       " highest_tf_level, lowest_exponent, oldest_exponent, no_p1_done (dctf only), lhm_bit_first"
                       " (lltf only), lhm_depth_first (lltf only), and let_gpu72_decide (let_gpu72_decide may override"
                       " max_exp).")

parser.add_option("-u", "--username", dest="username", help="Primenet user name")
parser.add_option("-p", "--password", dest="password", help="Primenet password")
parser.add_option("-w", "--workdir", dest="workdir", default=".",
                  help="Working directory with worktodo.txt and results.txt, default current")

parser.add_option("-U", "--gpu72user", dest="guser", help="GPU72 user name", default="")
parser.add_option("-P", "--gpu72pass", dest="gpass", help="GPU72 password")

parser.add_option("-n", "--num_cache", dest="num_cache", default="1",
                  help="Number of assignments to cache, default 1")
parser.add_option("-g", "--ghzd_cache", dest="ghzd_cache", default="",
                  help="GHz-days of assignments to cache, taking into account checkpoint files. Overrides num_cache.")
parser.add_option("-f", "--fallback", dest="fallback", default="1",
                  help="Fall back to mersenne.org when GPU72 fails or has no work, default 1.")

parser.add_option("-t", "--timeout", dest="timeout", default="3600",
                  help="Seconds to wait between network updates, default 3600. Use 0 for a single update without"
                       " looping.")

(options, args) = parser.parse_args()

use_gpu72 = (len(options.guser) > 0)

progname = os.path.basename(sys.argv[0])
workdir = os.path.expanduser(options.workdir)
timeout = int(options.timeout)

workfile = os.path.join(workdir, "worktodo.txt")

resultsfile = os.path.join(workdir, "results.txt")

# A cumulative backup
sentfile = os.path.join(workdir, "results_sent.txt")

# Trial factoring
workpattern = r"Factor=[^,]*(,[0-9]+){3}"

# mersenne.org limit is about 4 KB; stay on the safe side
sendlimit = 3000

# adapted from http://stackoverflow.com/questions/923296/keeping-a-session-in-python-while-making-http-requests
primenet_cj = cookielib.CookieJar()
primenet = urllib2.build_opener(urllib2.HTTPCookieProcessor(primenet_cj))

if use_gpu72:
    # Basic http auth
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, gpu72_baseurl + "account/", options.guser, options.gpass)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    gpu72 = urllib2.build_opener(handler)

while True:
    # Log in to primenet
    try:
        login_data = {"user_login": options.username, "user_password": options.password}

        # This makes a POST instead of GET
        data = urllib.urlencode(login_data)
        r = primenet.open(primenet_baseurl + "default.php", data)

        if not options.username + "<br>logged in" in r.read():
            primenet_login = False
            debug_print("Login failed.")
        else:
            primenet_login = True
            while submit_work() == "locked":
                debug_print("Waiting for results file access...")
                sleep(2)

    except urllib2.URLError:
        debug_print("Primenet URL open error")

    while get_assignment() == "locked":
        debug_print("Waiting for worktodo.txt access...")
        sleep(2)

    if timeout <= 0:
        break

    sleep(timeout)
