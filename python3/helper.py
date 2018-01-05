import re
import os


def primenet_fetch_mfloop(num_to_get):
    if not primenet_login:
        return []

    # Manual assignment settings; trial factoring = 2
    assignment = {"cores": "1",
                  "num_to_get": str(num_to_get),
                  "pref": "2",
                  "exp_lo": "",
                  "exp_hi": ""}

    try:
        r = primenet.open(primenet_baseurl + "manual_assignment/?" + ass_generate(assignment) + "B1=Get+Assignments")
        return exp_increase(greplike(workpattern, r.readlines()), int(options.max_exp))
    except urllib2.URLError:
        debug_print("URL open error at primenet_fetch")
        return []


def primenet_fetch_llloop(num_to_get):
    global primenet_login
    if not primenet_login:
        return []
    # <option value="102">World record tests
    # <option value="100">Smallest available first-time tests
    # <option value="101">Double-check tests
    assignment = {"cores": "1",
                  "num_to_get": str(num_to_get),
                  "pref": options.worktype,
                  "exp_lo": "",
                  "exp_hi": ""}
    try:
        r = primenet.open(primenet_baseurl + "manual_assignment/?" + ass_generate(assignment) + "B1=Get+Assignments")
        return greplike(workpattern, r.readlines())
    except urllib2.URLError:
        print("URL open error at primenet_fetch")
        return []


def get_assignment_mfloop():
    w = read_list_file(workfile)
    if w == "locked":
        return "locked"

    fetch = {True: gpu72_fetch, False: primenet_fetch}

    tasks = greplike(workpattern, w)

    if use_gpu72 and options.ghzd_cache != "":
        ghzd_to_get = ghzd_topup(tasks, int(options.ghzd_cache))
        num_to_get = 0
    else:
        ghzd_to_get = 0
        num_to_get = num_topup(tasks, int(options.num_cache))

    if num_to_get < 1 and ghzd_to_get == 0:
        debug_print("Cache full, not getting new work")
        # Must write something anyway to clear the lockfile
        new_tasks = []
    else:
        if use_gpu72 and ghzd_to_get > 0:
            debug_print("Fetching " + str(ghzd_to_get) + " GHz-days of assignments")
            new_tasks = fetch[use_gpu72](num_to_get, ghzd_to_get)
        else:
            debug_print("Fetching " + str(num_to_get) + " assignments")
            new_tasks = fetch[use_gpu72](num_to_get)

        # Fallback to primenet in case of problems
        if use_gpu72 and options.fallback == "1" and num_to_get and len(new_tasks) == 0:
            debug_print("Error retrieving from gpu72.")
            new_tasks = fetch[not use_gpu72](num_to_get)

    write_list_file(workfile, new_tasks, "a")


def get_assignment_llloop():
    w = read_list_file(workfile)
    if w == "locked":
        return "locked"
    tasks = greplike(workpattern, w)
    tasks_keep = []
    # If the work is finished, remove it from tasks
    tasks = filter(unfinished, tasks)
    num_to_get = num_topup(tasks, options.num_cache)
    if num_to_get < 1:
        print("Cache full, not getting new work")
    else:
        print("Fetching " + str(num_to_get) + " assignments")
        tasks += primenet_fetch(num_to_get)
    # Output work for cllucas
    if len(tasks) > 0:
        mersenne = mersenne_find_task(tasks[0])
    else:
        mersenne = ""
    write_list_file(workfile, tasks)
    return mersenne


def submit_work_mfloop():
    # Only submit completed work, i.e. the exponent must not exist in
    # worktodo.txt any more

    files = [resultsfile, sentfile]
    rs = map(read_list_file, files)

    if "locked" in rs:
        # Remove the lock in case one of these was unlocked at start
        for i in range(len(files)):
            if rs[i] != "locked":
                write_list_file(files[i], [], "a")

        return "locked"

    results = rs[0]

    # Only for new results, to be appended to results_sent
    sent = []

    # Use the textarea form to submit several results at once.

    # Useless lines (not including a M#) are now discarded completely.

    results_send = filter(mersenne_find, results)
    results_keep = filter(lambda x: mersenne_find(x, complete=False), results)

    if len(results_send) == 0:
        debug_print("No complete results found to send.")
        # Don't just return here, files are still locked...
    else:
        while len(results_send) > 0:
            sendbatch = []
            while sum(map(len, sendbatch)) < sendlimit and len(results_send) > 0:
                sendbatch.append(results_send.pop(0))

            data = "\n".join(sendbatch)

            debug_print("Submitting\n" + data)

            try:
                post_data = urllib.urlencode({"data": data})
                r = primenet.open(primenet_baseurl + "manual_result/default.php", post_data)
                res = r.read()
                if "processing:" in res or "Accepted" in res:
                    sent += sendbatch
                else:
                    results_keep += sendbatch
                    debug_print("Submission failed.")
            except urllib2.URLError:
                results_keep += sendbatch
                debug_print("URL open error")

    write_list_file(resultsfile, results_keep)
    write_list_file(sentfile, sent, "a")


def submit_work_llloop():
    # There is no concept of incomplete results, as in mfloop.py, so
    # we simply send every sensible line in resultsfile. But only
    # delete after a succesful send, and even those are backed up to
    # sentfile.
    files = [resultsfile, sentfile]
    rs = map(read_list_file, files)
    if "locked" in rs:
        # Remove the lock in case one of these was unlocked at start
        for i in range(len(files)):
            if rs[i] != "locked":
                write_list_file(files[i], [], "a")
        return "locked"
    results = rs[0]
    # Only for new results, to be appended to sentfile
    sent = []
    # Example: M( 110503 )P, n = 6144, clLucas v1.00
    results_send = greplike(r"M\( ([0-9]*) \).*", results)
    results_keep = []
    # Use the textarea form to submit several results at once.
    if len(results_send) == 0:
        print("No complete results found to send.")
        # Don't just return here, files are still locked...
    else:
        while len(results_send) > 0:
            sendbatch = []
            while sum(map(len, sendbatch)) < sendlimit and len(results_send) > 0:
                sendbatch.append(results_send.pop(0))
            data = "\n".join(sendbatch)
            print("Submitting\n" + data)
            try:
                r = primenet.open(primenet_baseurl + "manual_result/default.php?data=" + cleanup(data) + "&B1=Submit")
                if "processing:" in r.read():
                    sent += sendbatch
                else:
                    results_keep += sendbatch
                    print("Submission failed.")
            except urllib2.URLError:
                results_keep += sendbatch
                print("URL open error")
    write_list_file(resultsfile, results_keep)
    write_list_file(sentfile, sent, "a")


def ass_generate(assignment):
    output = ""
    for key in assignment:
        output += key + "=" + assignment[key] + "&"
    # return output.rstrip("&")
    return output


def cleanup(data):
    # as in submit_spider; urllib2.quote does not quite work here
    output = re.sub(" ", "+", data)
    output = re.sub(":", "%3A", output)
    output = re.sub(",", "%2C", output)
    output = re.sub("\n", "%0A", output)
    return output


def debug_print(text):
    if options.debug:
        print(progname + ": " + text)


def greplike(pattern, l):
    output = []
    for line in l:
        s = re.search(r"(" + pattern + ")$", line)
        if s:
            output.append(s.groups()[0])
    return output


def num_topup(l, targetsize):
    num_existing = len(l)
    num_needed = targetsize - num_existing
    return max(num_needed, 0)


def readonly_file(filename):
    # Used when there is no intention to write the file back, so don't
    # check or write lockfiles. Also returns a single string, no list.
    if os.path.exists(filename):
        file = open(filename, "r")
        contents = file.read()
        file.close()
    else:
        contents = ""

    return contents


def read_list_file(filename):
    # Used when we plan to write the new version, so use locking
    lockfile = filename + ".lck"

    try:
        fd = os.open(lockfile, os.O_CREAT | os.O_EXCL)
        os.close(fd)

        if os.path.exists(filename):
            file = open(filename, "r")
            contents = file.readlines()
            file.close()
            return map(lambda x: x.rstrip(), contents)
        else:
            return []

    except OSError:
        if OSError.errno == 17:
            return "locked"
        else:
            raise


def write_list_file(filename, l, mode="w"):
    # Assume we put the lock in upon reading the file, so we can
    # safely write the file and remove the lock
    lockfile = filename + ".lck"

    # A "null append" is meaningful, as we can call this to clear the
    # lockfile. In this case the main file need not be touched.
    if mode != "a" or len(l) > 0:
        content = "\n".join(l) + "\n"
        file = open(filename, mode)
        file.write(content)
        file.close()

    os.remove(lockfile)


def exp_increase(l, max_exp):
    output = []
    for line in l:
        # Increase the upper limit to max_exp
        s = re.search(r",([0-9]+)$", line)
        if s:
            exp = int(s.groups()[0])
            new_exp = str(max(exp, max_exp))
            output.append(re.sub(r",([0-9]+)$", "," + new_exp, line))
    return output


def ghzd_topup(l, ghdz_target):
    ghzd_existing = 0.0
    for line in l:
        pieces = line.split(",")
        # calculate ghz-d http://mersenneforum.org/showpost.php?p=152280&postcount=204
        exponent = int(pieces[1])
        first_bit = int(pieces[2]) + 1
        for bits in range(first_bit, int(pieces[3]) + 1):
            if bits > 65:
                timing = 28.50624  # 2.4 * 0.00707 * 1680.0
            elif bits == 64:
                timing = 28.66752  # 2.4 * 0.00711 * 1680.0
            elif bits == 63 or bits == 62:
                timing = 29.95776  # 2.4 * 0.00743 * 1680.0
            elif bits >= 48:
                timing = 18.7488  # 2.4 * 0.00465 * 1680.0
            else:
                continue

            bit_ghzd = timing * (1 << (bits - 48)) / exponent

            # if there is a checkpoint file, subtract the work done
            if bits == first_bit:
                checkpoint_file = os.path.join(workdir, "M"+str(exponent)+".ckp")
                if os.path.isfile(checkpoint_file):
                    file = open(checkpoint_file, "r")
                    checkpoint = file.readline()
                    file.close()
                    checkpoint_pieces = checkpoint.split(" ")
                    if checkpoint_pieces[4] == "mfakto":
                        progress_index = 6
                    else:
                        progress_index = 5

                    percent_done = float(checkpoint_pieces[progress_index]) / float(checkpoint_pieces[3])
                    bit_ghzd *= 1 - percent_done
                    debug_print("Found checkpoint file for assignment M" + str(exponent) +
                                " indicating " + str(round(percent_done*100, 2)) + "% done.")

            ghzd_existing += bit_ghzd

    debug_print("Found " + str(ghzd_existing) + " of existing GHz-days of work")

    return max(0, math.ceil(ghdz_target - ghzd_existing))


def gpu72_fetch(num_to_get, ghzd_to_get=0):
    if options.gpu72_type == "dctf":
        gpu72_type = "dctf"
    else:
        gpu72_type = "lltf"

    if options.gpu72_option == "lowest_tf_level":
        option = "1"
    elif options.gpu72_option == "highest_tf_level":
        option = "2"
    elif options.gpu72_option == "lowest_exponent":
        option = "3"
    elif options.gpu72_option == "oldest_exponent":
        option = "4"
    elif gpu72_type == "dctf" and options.gpu72_option == "no_p1_done":
        option = "5"
    elif gpu72_type == "lltf" and options.gpu72_option == "lhm_bit_first":
        option = "6"
    elif gpu72_type == "lltf" and options.gpu72_option == "lhm_depth_first":
        option = "7"
    elif options.gpu72_option == "let_gpu72_decide":
        option = "9"
    else:
        option = "0"

    if ghzd_to_get > 0:
        num_to_get_str = "0"
        ghzd_to_get_str = str(ghzd_to_get)
    else:
        num_to_get_str = str(num_to_get)
        ghzd_to_get_str = ""

    assignment = {"Number": num_to_get_str,
                  "GHzDays": ghzd_to_get_str,
                  "Low": "0",
                  "High": "10000000000",
                  "Pledge": str(max(70, int(options.max_exp))),
                  "Option": option}

    # This makes a POST instead of GET
    data = urllib.urlencode(assignment)
    req = urllib2.Request(gpu72_baseurl + "account/getassignments/" + gpu72_type + "/", data)

    try:
        r = gpu72.open(req)
        new_tasks = greplike(workpattern, r.readlines())
        # Remove dupes
        return list(set(new_tasks))

    except urllib2.URLError:
        debug_print("URL open error at gpu72_fetch")

    return []


def mersenne_find(line, complete=True):
    work = readonly_file(workfile)

    s = re.search(r"M([0-9]*) ", line)
    if s:
        mersenne = s.groups()[0]
        if not "," + mersenne + "," in work:
            return complete
        else:
            return not complete
    else:
        return False


def mersenne_find_task(line):
    s = re.search(r",([0-9]+),[0-9]+,[0-9]+", line)
    if s:
        return s.groups()[0]
    else:
        return ""


def unfinished(line):
    finished = readonly_file(resultsfile)
    mersenne = mersenne_find_task(line)
    if len(mersenne) > 0 and "( " + mersenne + " )" in finished:
        return False
    else:
        return True


def fft_opt(m):
    # Optimal FFT size for clLucas
    if int(m) > 38000000:
        fft = 4096
    else:
        fft = 2048
    # clLucas 1.04 has automatic size incrementing. This script can
    # still be useful for finding more optimal values, but in the
    # meantime, start with something basic.
    fft = 2048
    # Format for clLucas
    return ["-f", str(fft) + "K"]


def network_getwork():
    global options, primenet, primenet_baseurl, primenet_login
    mersenne = ""
    try:
        # Log in to primenet
        login_data = {"user_login": options.username,
                      "user_password": options.password}
        # This makes a POST instead of GET
        data = urllib.urlencode(login_data)
        r = primenet.open(primenet_baseurl + "default.php", data)

        primenet_login = options.username + "<br>logged in" in r.read()

        # The order of get_assignment, then submit_work is important,
        # because we check resultsfile for finished work when handling
        # workfile. clLucas does not use lockfiles, so in the present
        # form we can ignore them.

        # This doesn't require login as it also gets tasks from the
        # file cache.
        mersenne = get_assignment()

        if primenet_login:
            submit_work()
        else:
            print("Login failed.")
    except urllib2.URLError:
        print("Primenet URL open error")
    return mersenne
