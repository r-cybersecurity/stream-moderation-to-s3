import argparse, logging, os, requests, time
from config import *
from utilities import DaemonBuilder


class StreamComments(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 2

    def implement_run(self):
        for comment in self.reddit.subreddit(args.subreddit).stream.comments():
            self.is_item_ok(comment, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/comment/{comment.id}/{epoch}.zz"
            self.save(key, comment)
            self.confirm(comment)


class StreamEditedComments(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 12

    def implement_run(self):
        for comment in self.reddit.subreddit(args.subreddit).mod.stream.edited(
            "comments"
        ):
            self.is_item_ok(comment, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/comment/{comment.id}/{epoch}-edited.zz"
            self.save(key, comment)
            self.confirm(comment)


class StreamReportedComments(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 24

    def implement_run(self):
        for comment in self.reddit.subreddit(args.subreddit).mod.stream.reports(
            "comments"
        ):
            self.is_item_ok(comment, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/comment/{comment.id}/{epoch}-reported.zz"
            self.save(key, comment)
            self.confirm(comment)


class StreamSpammedComments(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 48

    def implement_run(self):
        for comment in self.reddit.subreddit(args.subreddit).mod.stream.spam(
            "comments"
        ):
            self.is_item_ok(comment, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/comment/{comment.id}/{epoch}-spam.zz"
            self.save(key, comment)
            self.confirm(comment)

class StreamSubmissions(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 4

    def implement_run(self):
        for submission in self.reddit.subreddit(args.subreddit).stream.submissions():
            self.is_item_ok(submission, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/submission/{submission.id}/{epoch}.zz"
            self.save(key, submission)
            self.confirm(submission)


class StreamEditedSubmissions(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 24

    def implement_run(self):
        for submission in self.reddit.subreddit(args.subreddit).mod.stream.edited(
            "submissions"
        ):
            self.is_item_ok(submission, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/submission/{submission.id}/{epoch}-edited.zz"
            self.save(key, submission)
            self.confirm(submission)


class StreamReportedSubmissions(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 8

    def implement_run(self):
        for submission in self.reddit.subreddit(args.subreddit).mod.stream.reports(
            "submissions"
        ):
            self.is_item_ok(submission, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/submission/{submission.id}/{epoch}-reported.zz"
            self.save(key, submission)
            self.confirm(submission)


class StreamSpammedSubmissions(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 4

    def implement_run(self):
        for submission in self.reddit.subreddit(args.subreddit).mod.stream.spam(
            "submissions"
        ):
            self.is_item_ok(submission, ["id"])
            epoch = round(time.time())
            key = f"{args.subreddit}/submission/{submission.id}/{epoch}-spam.zz"
            self.save(key, submission)
            self.confirm(submission)


class StreamModlog(DaemonBuilder):
    def __init__(self):
        super().__init__()
        self.tolerance = 60 * 60 * 4

    def implement_run(self):
        for log in self.reddit.subreddit(args.subreddit).mod.stream.log():
            self.is_item_ok(log, ["id", "created_utc"])
            key = f"{args.subreddit}/modlog/{round(log.created_utc)}-{log.id}.zz"
            self.save(key, log)
            self.confirm(log)


parser = argparse.ArgumentParser(
    description=("Streams moderation data from Reddit to S3 for archival")
)
parser.add_argument(
    "-b",
    "--beat",
    type=str,
    help="A URL to request which indicates a healthy heartbeat",
)
parser.add_argument(
    "-s",
    "--subreddit",
    type=str,
    help="What subreddit to monitor all feeds for",
)
parser.add_argument(
    "-l",
    "--log",
    type=str,
    help="File to write logs to",
)
parser.add_argument(
    "-d", "--debug", help="Output a metric shitton of runtime data", action="store_true"
)
parser.add_argument(
    "-v",
    "--verbose",
    help="Output a reasonable amount of runtime data",
    action="store_true",
)

args = parser.parse_args()

if args.debug:
    log_level = logging.DEBUG
elif args.verbose:
    log_level = logging.INFO
else:
    log_level = logging.WARNING

if args.log:
    logging.basicConfig(
        filename=args.log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=log_level,
    )
else:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s", level=log_level
    )
logger = logging.getLogger()

daemons = []
daemons.append(StreamComments())
daemons.append(StreamEditedComments())
daemons.append(StreamReportedComments())
daemons.append(StreamSpammedComments())
daemons.append(StreamSubmissions())
daemons.append(StreamEditedSubmissions())
daemons.append(StreamReportedSubmissions())
daemons.append(StreamSpammedSubmissions())
daemons.append(StreamModlog())

for daemon in daemons:
    logger.info(f"Starting {daemon.name} thread")
    daemon.start()

while True:
    time.sleep(30)
    report_healthy = True
    for daemon in daemons:
        if not daemon.is_alive():
            os._exit(1)

        headroom = round(
            (daemon.last_epoch + daemon.tolerance - time.time()) / (60 * 60), 2
        )
        if daemon.last_epoch + daemon.tolerance < time.time():
            logger.warning(f"{daemon.name} may be out of date ({str(headroom)}h)")
            report_healthy = False
        else:
            logger.info(f"{daemon.name} appears to be up-to-date ({str(headroom)}h)")

    if report_healthy and args.beat:
        logger.info(f"Sending heartbeat because all daemons appear to be up-to-date")
        try:
            requests.get(args.beat, timeout=5)
        except Exception as e:
            logger.warning(f"Heartbeat was not successful, got error {e}")
