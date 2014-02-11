

from twisted.trial.unittest import TestCase

from otter.worker.heat_template import lc_to_resource, generate_template



class LaunchConfigToResourceSpecTests(TestCase):

    def test_what(self):
        lc = {
            'type': 'launch_server',
            'args': {
                'server': {
                    'imageRef': 'my-image',
                    'flavorRef': 'my-flavor',
                    'OS-DCF:diskConfig': 'my-disk-config',
                    'other-prop': 'my-other',
                }
            }
        }
        expected = {
            'type': 'OS::Nova::Server',
            'properties': {
                'image': 'my-image',
                'flavor': 'my-flavor',
                'diskConfig': 'my-disk-config',
                'other-prop': 'my-other'
            }
        }

        self.assertEqual(lc_to_resource(lc), expected)
    
    def test_template(self):
        lc = {
            'type': 'launch_server',
            'args': {
                'server': {
                    'imageRef': 'my-image',
                    'flavorRef': 'my-flavor',
                    'OS-DCF:diskConfig': 'my-disk-config',
                    'other-prop': 'my-other',
                }
            }
        }
        resource = {
            'type': 'OS::Nova::Server',
            'properties': {
                'image': 'my-image',
                'flavor': 'my-flavor',
                'diskConfig': 'my-disk-config',
                'other-prop': 'my-other'
            }
        }
        expected = {
            'resources': {
                'server-0': resource,
                'server-1': resource,
                'server-2': resource
            }
        }
        
        self.assertEqual(generate_template(lc, 3), expected)
