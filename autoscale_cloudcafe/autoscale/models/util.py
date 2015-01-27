"""
Useful utilities to take the place of cloudcafe functionality.
"""

from toolz.dicttoolz import itemfilter


class BaseModel(object):
    """
    Class that attempts to replicate some limited functionality of
    Cloudcafe BaseModel.  This is pretty pretty terrible.
    We should just eliminate all the models or convert them to characteristic,
    but this is the smallest change to remove Cloudcafe.
    """

    def _auto_to_dict(self):
        """
        Pretty terrible way to convert objects into dictionaries, and also
        strip out any null values (to maintain compatibility with Cloudcafe.)
        """
        return itemfilter(
            lambda i: i[0] != 'characteristic_attributes' and i[1] is not None,
            self.__dict__)

    def __eq__(self, other):
        """
        If the attributes are the same, return true.
        """
        return (isinstance(other, self.__class__) and
                self.__dict__ == other.__dict__)
