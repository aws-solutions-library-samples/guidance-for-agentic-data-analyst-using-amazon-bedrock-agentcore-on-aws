"""Register a process-wide botocore hook that appends the AWS Solutions usage
tracking token to the User-Agent of every AWS SDK (boto3/botocore) call this
process makes.

The token is read from the ``USER_AGENT_STRING`` environment variable, which
the CDK stack sets from the single ``SOLUTION_USER_AGENT`` declaration. If the
variable is not set this module is a no-op.

Import this module before any other module that talks to AWS. It is delivered
to Lambda functions as a shared layer, so it is defined in exactly one place.
"""
import os

import botocore.session

SOLUTION_UA = os.environ.get("USER_AGENT_STRING")

if SOLUTION_UA:
    _orig_init = botocore.session.Session.__init__

    def _init(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        extra = self.user_agent_extra or ""
        if SOLUTION_UA not in extra.split():
            self.user_agent_extra = f"{extra} {SOLUTION_UA}".strip()

    botocore.session.Session.__init__ = _init
