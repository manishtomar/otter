

from twisted.trial.unittest import TestCase

from otter.worker.heat_template import lc_to_resource



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