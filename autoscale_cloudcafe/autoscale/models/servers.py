"""
Marshalling for server objects
"""
import json

from characteristic import attributes, Attribute

from autoscale.models.util import BaseModel


class Metadata(BaseModel):

    """
    :summary: Metadata Request Object for Server
    """
    def __init__(self, metadata_dict):
        super(Metadata, self).__init__()
        for key, value in metadata_dict.items():
            setattr(self, key, value)

    def _obj_to_json(self):
        ret = self._auto_to_dict()
        return json.dumps(ret)

    @classmethod
    def _dict_to_obj(cls, metadata_dict):
        """
        :summary: Initializes the object from json response
        :param metadata_dict: metadata details
        :type metadata_dict: dictionary
        """
        return Metadata(metadata_dict)

    @classmethod
    def _json_to_obj(cls, serialized_str):
        """
        Returns an instance of metadata based on the json
        serialized_str passed in
        """
        json_dict = json.loads(serialized_str)
        if 'metadata' in json_dict.keys():
            metadata_dict = json_dict['metadata']
            return Metadata(metadata_dict)

    @classmethod
    def _obj_to_dict(self, meta_obj):
        meta = {}
        for name in dir(meta_obj):
            value = getattr(meta_obj, name)
            if not name.startswith('_') and not \
                name.startswith('RO') and not name.startswith('deser')\
                    and not name.startswith('sele') and not name.startswith('seria'):
                meta[name] = value
        return meta


class Links(BaseModel):

    """
    :summary: Represents links (url) in the system
    """
    ROOT_TAG = 'links'

    def __init__(self, links_list):
        super(Links, self).__init__()
        self.links = {}
        if links_list is not None:
            for link in links_list:
                self.links[link['rel']] = link['href']
            for key_name in self.links:
                setattr(self, key_name, self.links[key_name])

    @classmethod
    def _json_to_obj(cls, serialized_str):
        """Returns an instance of links based on the json
        serialized_str passed in."""
        json_dict = json.loads(serialized_str)
        if 'links' in json_dict.keys():
            links_list = json_dict['links']
            return Links(links_list)

    @classmethod
    def _dict_to_obj(cls, list_of_links):
        """
        :summary: Initializes the object from json response
        :param list_of_links: links details
        :type list_of_links: list
        """
        return Links(list_of_links)


class Network(BaseModel):

    """
    :summary: Represents networks in the system
    """

    def __init__(self, **kwargs):
        super(Network, self).__init__()
        for keys, values in kwargs.items():
            setattr(self, keys, values)

    @classmethod
    def _json_to_obj(cls, serialized_str):
        """Returns an instance of networks based on the json
        serialized_str passed in."""
        ret = []
        for k in serialized_str:
            s = cls._dict_to_obj(k)
            ret.append(s)
        return ret

    @classmethod
    def _dict_to_obj(cls, network_dict):
        network = Network(**network_dict)
        if hasattr(network, 'uuid'):
            setattr(network, 'uuid', getattr(network, 'uuid'))
        return network


class Personality(BaseModel):

    """
    :summary: Represents networks in the system
    """

    def __init__(self, **kwargs):
        super(Personality, self).__init__()
        for keys, values in kwargs.items():
            setattr(self, keys, values)

    @classmethod
    def _json_to_obj(cls, serialized_str):
        """
        Returns an instance of personalitys based on the json
        serialized_str passed in
        """
        ret = []
        for k in serialized_str:
            s = cls._dict_to_obj(k)
            ret.append(s)
        return ret

    @classmethod
    def _dict_to_obj(cls, personality_dict):
        personality = Personality(**personality_dict)
        if hasattr(personality, 'path'):
            setattr(personality, 'path', getattr(personality, 'path'))
        if hasattr(personality, 'content'):
            setattr(personality, 'content', getattr(personality, 'content'))
        return personality


@attributes(['id', 'links', 'name',
             Attribute('disk_config', default_value=None),
             Attribute('power_state', default_value=None),
             Attribute('progress', default_value=None),
             Attribute('task_state', default_value=None),
             Attribute('vm_state', default_value=None),
             Attribute('name', default_value=None),
             Attribute('tenant_id', default_value=None),
             Attribute('status', default_value=None),
             Attribute('updated', default_value=None),
             Attribute('created', default_value=None),
             Attribute('host_id', default_value=None),
             Attribute('user_id', default_value=None),
             Attribute('accessIPv4', default_value=None),
             Attribute('accessIPv6', default_value=None),
             Attribute('addresses', default_value=None),
             Attribute('flavor', default_value=None),
             Attribute('image', default_value=None),
             Attribute('links', default_value=None),
             Attribute('metadata', default_value=None),
             Attribute('key_name', default_value=None),
             Attribute('config_drive', default_value=None),
             Attribute('instance_name', default_value=None)])
class Server(BaseModel):
    """
    Server automarshalling model copied and simplified from cloudcafe - this
    only provides what the autoscale integration tests need, and no more.
    """

    @classmethod
    def _json_to_obj(cls, serialized_str):
        """
        Returns an instance of a :class:`Server` based on the json
        serialized_str passed in, or a list of :class:`Server` if it's a list
        of servers.
        """
        json_dict = json.loads(serialized_str)
        if 'server' in json_dict:
            return cls._dict_to_obj(json_dict['server'])
        elif 'servers' in json_dict:
            return [cls._dict_to_obj(s) for s in json_dict['servers']]
        return None

    @classmethod
    def _dict_to_obj(cls, server_dict):
        """Helper method to turn dictionary into Server instance."""

        addresses = None
        flavor = None
        image = None
        links = None
        metadata = None

        if 'links' in server_dict:
            links = Links._dict_to_obj(server_dict['links'])
        if 'addresses' in server_dict:
            addresses = Addresses(**server_dict['addresses'])
        if 'flavor' in server_dict:
            flavor = Flavor(**server_dict['flavor'])
        if 'image' in server_dict and server_dict.get('image'):
            image = Image(**server_dict['image'])
        if 'metadata' in server_dict:
            metadata = Metadata._dict_to_obj(server_dict['metadata'])

        server = Server(
            id=server_dict.get('id') or server_dict.get('uuid'),
            disk_config=server_dict.get('OS-DCF:diskConfig'),
            power_state=server_dict.get('OS-EXT-STS:power_state'),
            progress=server_dict.get('progress', 0),
            task_state=server_dict.get('OS-EXT-STS:task_state'),
            vm_state=server_dict.get('OS-EXT-STS:vm_state'),
            name=server_dict.get('name'),
            config_drive=server_dict.get('config_drive'),
            tenant_id=server_dict.get('tenant_id'),
            status=server_dict.get('status'),
            updated=server_dict.get('updated'),
            created=server_dict.get('created'),
            host_id=server_dict.get('hostId'),
            user_id=server_dict.get('user_id'),
            accessIPv4=server_dict.get('accessIPv4'),
            accessIPv6=server_dict.get('accessIPv6'), addresses=addresses,
            flavor=flavor, image=image, links=links, metadata=metadata,
            key_name=server_dict.get('key_name'))

        return server


@attributes(["id"])
class Image(object):
    """
    Simplified image automarshalling model needed by :class:`Server` model.
    """
    def __init__(self, **kwargs):
        """
        Ignore any other params passed to the original init.
        """


@attributes(["id"])
class Flavor(object):
    """
    Simplified flavor automarshalling model needed by :class:`Server` model.
    """
    def __init__(self, **kwargs):
        """
        Ignore any other params passed to the original init.
        """


@attributes(["version", "addr"])
class _AddrObj(BaseModel):
    """
    Simplified network address object needed by :class:`Addresses` model
    """


@attributes(["addresses"])
class _NetworkAddressesList(BaseModel):
    """
    Simplified network address list object needed by :class:`Addresses` model
    """


class Addresses(object):
    def __init__(self, **addresses):
        """
        Apparently all the attributes on this object are network names.
        """
        # Preset properties that should be expected, if not always populated
        self.public = None
        self.private = None

        for addr_name, addr_list in addresses.iteritems():
            # >:(  why the objects in objects in objects I don't know
            addresses = [_AddrObj(**addr_dict) for addr_dict in addr_list]
            setattr(self, addr_name,
                    _NetworkAddressesList(addresses=addresses))

    @classmethod
    def _json_to_obj(cls, serialized_str):
        json_dict = json.loads(serialized_str)
        if 'addresses' in json_dict:
            return Addresses(**json_dict['addresses'])
        return Addresses(**json_dict)
