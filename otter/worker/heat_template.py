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

