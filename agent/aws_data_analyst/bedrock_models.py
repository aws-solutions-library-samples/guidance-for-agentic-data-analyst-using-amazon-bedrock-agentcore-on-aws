# See: https://aws.amazon.com/bedrock/pricing/

CLAUDE_HAIKU_5_5_MODEL = {
    'id': "global.anthropic.claude-haiku-5-5",
    'cost': {
        'on_demand': {
            'input' : 0.0000001,
            'output': 0.0000005
        }
    }
}

CLAUDE_SONNET_5_5_MODEL = {
    'id': "global.anthropic.claude-sonnet-5-5",
    'cost': {
        'on_demand': {
            'input' : 0.000002,
            'output': 0.00001
        }
    }
}

CLAUDE_OPUS_5_5_MODEL = {
    'id': "global.anthropic.claude-opus-5-5",
    'cost': {
        'on_demand': {
            'input' : 0.000004,
            'output': 0.00002
        }
    }
}


DEFAULT_MODEL_ID = CLAUDE_OPUS_5_5_MODEL['id']
DEFAULT_TEMPERATURE = 0.1


MODELS = {
    model['id']: model for model in [
        CLAUDE_HAIKU_5_5_MODEL,
        CLAUDE_SONNET_5_5_MODEL,
        CLAUDE_OPUS_5_5_MODEL,
    ]
}
