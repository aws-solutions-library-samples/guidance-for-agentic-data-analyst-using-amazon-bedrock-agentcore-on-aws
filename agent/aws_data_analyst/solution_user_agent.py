"""Register a process-wide botocore hook that appends the AWS Solutions usage
tracking token to the User-Agent of every AWS SDK (boto3/botocore) call this
process makes, including clients Strands creates internally.

Import this module before any other module that talks to AWS.
"""
import os

import botocore.session

_DEFAULT_SOLUTION_UA = "AWSSOLUTION/SO0354/v0.1.0"
SOLUTION_UA = os.environ.get("USER_AGENT_STRING") or _DEFAULT_SOLUTION_UA

_orig_init = botocore.session.Session.__init__


def _init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    extra = self.user_agent_extra or ""
    if SOLUTION_UA and SOLUTION_UA not in extra.split():
        self.user_agent_extra = f"{extra} {SOLUTION_UA}".strip()


botocore.session.Session.__init__ = _init
