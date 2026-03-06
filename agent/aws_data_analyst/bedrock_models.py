# See: https://aws.amazon.com/bedrock/pricing/

MINMAX_M2_MODEL = {
    'id': "minimax.minimax-m2",
    'cost': {
        'on_demand': {
            'input' : 0.0000003,
            'output': 0.0000012
        }
    }
}
NOVA_LITE_2_MODEL = {
    'id': "global.amazon.nova-2-lite-v1:0",
    'cost': {
        'on_demand': {
            'input' : 0.00000015,
            'output': 0.00000125
        }
    }
}
NOVA_PRO_2_MODEL = {
    'id': "us.amazon.nova-2-pro-preview-20251202-v1:0",
    'cost': {
        'on_demand': {
            'input' : 0.00000125,
            'output': 0.00001
        }
    }
}
CLAUDE_HAIKU_4_5_MODEL = {
    'id': "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    'cost': {
        'on_demand': {
            'input' : 0.000001,
            'output': 0.000005
        }
    }
}
CLAUDE_SONNET_4_6_MODEL = {
    'id': "global.anthropic.claude-sonnet-4-6",
    'cost': {
        'on_demand': {
            'input' : 0.000003,
            'output': 0.000015
        }
    }
}
CLAUDE_OPUS_4_6_MODEL = {
    'id': "global.anthropic.claude-opus-4-6-v1",
    'cost': {
        'on_demand': {
            'input' : 0.000005,
            'output': 0.000025
        }
    }
}
QWEN3_CODER_30B_A3B = {
    'id': "qwen.qwen3-coder-30b-a3b-v1:0",
    'cost': {
        'on_demand': {
            'input' : 0.00000015,
            'output': 0.0000006
        }
    }
}

DEFAULT_MODEL_ID = CLAUDE_OPUS_4_6_MODEL['id']
DEFAULT_TEMPERATURE = 0.1


MODELS = {
    model['id']: model for model in [
        MINMAX_M2_MODEL,
        CLAUDE_HAIKU_4_5_MODEL,
        CLAUDE_SONNET_4_6_MODEL,
        CLAUDE_OPUS_4_6_MODEL]
}
