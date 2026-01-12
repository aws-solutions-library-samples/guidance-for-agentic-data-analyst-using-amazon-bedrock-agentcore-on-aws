import boto3


ssm = boto3.client('ssm')


def get_infrastructure_param(name):
    return ssm.get_parameter(Name=f"/data-analyst/{name}")['Parameter']['Value']


def get_vectordb_configuration(dev=False):
    name_postfix = '-dev' if dev else ''
    return {
        'embedder_id': get_infrastructure_param('vectordb_embedder'),
        'embedding_dimension': int(get_infrastructure_param('vectordb_dimension')),
        'bucket': get_infrastructure_param('vectordb_bucket' + name_postfix),
        'index': get_infrastructure_param('vectordb_index' + name_postfix)
    }
