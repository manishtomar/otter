PROPERTY_RENAMES = {
    'imageRef': 'image',
    'flavorRef': 'flavor',
    'OS-DCF:diskConfig': 'diskConfig',
}


def rename_keys(mapping, renames):
    result = {}
    for key, val in mapping.iteritems():
        new_key = renames.get(key, key)
        result[new_key] = val
    return result


def lc_to_resource(lc):
    assert lc['type'] == 'launch_server'
    server = lc['args']['server']

    # metadata?
    resource_definition = {
        'type': 'OS::Nova::Server',
        'properties': rename_keys(server, PROPERTY_RENAMES)
    }
    return resource_definition


def generate_template(launch_config, desired_capacity):
    """
    Generates a template based on the launch config and the desired capacity
    """
    resource_definition = lc_to_resource(launch_config)
    name = launch_config['args']['server'].get('name', 'server')
    resources = {'{0}-{1}'.format(name, i): resource_definition
                 for i in range(desired_capacity)}
    # HEAT BUG: currently, without the version, the heat template version
    # gets coerced to 2012-12-12, for which 'resources' is invalid.
    #
    # The error returned says that 'resources' is an invalid section.
    # https://review.openstack.org/#/c/90109/  should address the issue.
    #
    # TODO: should generate YAML and use strict HOT syntax instead of json
    return {'heat_template_version': '2013-05-23', 'resources': resources}
