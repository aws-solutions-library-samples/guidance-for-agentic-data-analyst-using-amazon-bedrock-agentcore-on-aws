"""Single source of truth for this solution's AWS usage-tracking identifier.

``SOLUTION_USER_AGENT`` is injected into Lambda and runtime environments as the
``USER_AGENT_STRING`` variable and appended to the User-Agent of every AWS SDK
call so re-use of the solution can be measured.
"""
SOLUTION_ID = "SO0354"
SOLUTION_VERSION = "0.1.0"
SOLUTION_USER_AGENT = f"AWSSOLUTION/{SOLUTION_ID}/v{SOLUTION_VERSION}"
