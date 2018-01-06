#!/usr/bin/env python3
"""
Automatic assignment handler for manual testing at mersenne.org and optionally gpu72.com.
Written with mfakto in mind, this only handles trial factoring work for now. It should work with mfaktc as well.
This version can run in parallel with the factoring program, as it uses lockfiles to avoid conflicts when updating files.
"""

__author__ = 'teknohog and daxm'

import sys
import os.path
import requests
from time import sleep
import os
import urllib
import argparse

import userdata
from helper import submit_work_mfloop, debug_print, get_assignment_mfloop

primenet_baseurl = "https://www.mersenne.org/"
gpu72_baseurl = "https://www.gpu72.com/"


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("-d", "--debug", action="store_true", dest="debug", default=False,
                        help="Display debugging info")
    parser.add_argument("-w", "--workdir", dest="workdir", default=".",
                        help="Working directory with worktodo.txt and results.txt.\n"
                             "Default: current directory")
    parser.add_argument("-e", "--exp", dest="max_exp", default="72",
                        help="Upper limit of exponent.\n"
                             "Default: 72")
    parser.add_argument("-t", "--timeout", dest="timeout", default="3600",
                        help="Seconds to wait between network updates.\n"
                             "Default: 3600. (Use 0 for a single update without looping.)")
    parser.add_argument("-n", "--num_cache", dest="num_cache", default="1",
                        help="Number of assignments to cache.\n"
                             "Default: 1")
    parser.add_argument("-g", "--ghzd_cache", dest="ghzd_cache", default="",
                        help="GHz-days of assignments to cache, taking into account checkpoint files. Overrides num_cache.")
    parser.add_argument("-u", "--username", dest="username", default=userdata.username,
                        help="Primenet/GPU72 user name.\n"
                             "Default: username variable set in userdata.py")
    parser.add_argument("-p", "--password", dest="password", default=userdata.password,
                        help="Primenet/GPU72 password.\n"
                             "Default: password variable set in userdata.py")

    # GPU72 specific options
    parser.add_argument("-T", "--gpu72type", dest="gpu72_type", default="lltf",
                        help="GPU72 type of work, lltf or dctf.\n"
                             "Default: lltf")
    parser.add_argument("-o", "--gpu72option", dest="gpu72_option", default="what_makes_sense",
                        help="GPU72 Option to fetch.\n"
                             "Default: what_makes_sense\n"
                             "Valid options: what_makes_sense, lowest_tf_level, highest_tf_level, lowest_exponent, "
                             "oldest_exponent,\n no_p1_done (dctf only), lhm_bit_first (lltf only), "
                             "lhm_depth_first (lltf only),\n let_gpu72_decide (let_gpu72_decide overrides max_exp).")

    # Choose which service to get work from.
    parser.add_argument("-s", "--service", dest="service", default="primenet",
                        help="Which service to use.\n"
                             "Default: primnet\n"
                             "Valid options: primenet, gpu72.")
    parser.add_argument("-f", "--fallback", dest="fallback", default="1",
                        help="Fall back to mersenne.org when GPU72 fails or has no work.\n"
                             "Default: 1")

    args = parser.parse_args()

    # TODO:  These variables are referenced throughout the helper.py functions.  Need to deal with that.
    progname = os.path.basename(sys.argv[0])
    workdir = os.path.expanduser(args.workdir)
    timeout = int(args.timeout)
    workfile = os.path.join(workdir, "worktodo.txt")
    resultsfile = os.path.join(workdir, "results.txt")
    # A cumulative backup
    sentfile = os.path.join(workdir, "results_sent.txt")
    # Trial factoring
    workpattern = r"Factor=[^,]*(,[0-9]+){3}"
    # mersenne.org limit is about 4 KB; stay on the safe side
    sendlimit = 3000

    if args.service is 'gpu72':
        # Basic http auth
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, gpu72_baseurl + "account/", args.username, args.password)
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        gpu72 = urllib2.build_opener(handler)

    while True:
        # Log in to primenet
        with requests.Session() as session:
            # Log in
            login_data = {'user_login': args.username, 'user_password': args.password}
            result = session.post('{}/'.format(primenet_baseurl), data=login_data, )

        if -1 == result.text.find("{}<br>logged in".format(args.username)):
            debug_print(msg="Login failed.", debug=args.debug)
        else:
            while submit_work_mfloop() == "locked":
                debug_print("Waiting for results file access...")
                sleep(2)

        while get_assignment_mfloop() == "locked":
            debug_print("Waiting for worktodo.txt access...")
            sleep(2)

        if timeout <= 0:
            break

        sleep(timeout)


if __name__ == "__main__":
    main()
