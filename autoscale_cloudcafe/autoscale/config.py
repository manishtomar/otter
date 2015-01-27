"""
Define autoscale config values.
"""
from ConfigParser import SafeConfigParser
from characteristic import attributes, Attribute
from toolz.itertoolz import concat


@attributes(['tenant_id', 'region', 'gc_name', 'gc_cooldown',
             'gc_min_entities', 'gc_max_entities', 'gc_min_entities_alt',
             'lc_name', 'lc_flavor_ref', 'lc_image_ref', 'lc_image_ref_alt',
             'sp_name', 'sp_cooldown', 'sp_change', 'sp_policy_type',
             'upd_sp_change', 'sp_change_percent', 'sp_desired_capacity',
             'lc_load_balancers',
             'sp_list', 'wb_name', 'interval_time', 'timeout',
             'autoscale_endpoint_name', 'server_endpoint_name',
             'server_endpoint', 'load_balancer_endpoint_name',
             'non_autoscale_username', 'non_autoscale_password',
             'non_autoscale_tenant', 'autoscale_na_la_aa',
             'autoscale_na_lo_aa', 'autoscale_no_lo_aa',
             'autoscale_no_lo_ao', 'autoscale_na_la_ao',
             'autoscale_nc_lc_aa', 'autoscale_nc_lc_ao',
             'autoscale_na_la_ano', 'autoscale_nno_lno_ao',
             'autoscale_nno_lno_aa',
             Attribute('rcv3_endpoint_name', default_value=None),
             Attribute('rcv3_load_balancer_pool', default_value=None),
             Attribute('rcv3_cloud_network', default_value=None),
             Attribute('lbaas_region_override', default_value=None),
             Attribute('server_region_override', default_value=None),
             Attribute('rcv3_region_override', default_value=None),

             # maas stuff
             Attribute('check_type', default_value=None),
             Attribute('check_url', default_value=None),
             Attribute('check_method', default_value=None),
             Attribute('monitoring_zones', default_value=None),
             Attribute('check_timeout', default_value=None),
             Attribute('check_period', default_value=None),
             Attribute('target_alias', default_value=None),
             Attribute('alarm_criteria', default_value=None)])
class AutoscaleConfig(object):
    """
    Defines the config values for autoscale.

    Attribute('tenant_id', default_value=None)
    Attribute('region', default_value=None)
    Attribute('gc_name', default_value=None)
    Attribute('gc_cooldown', default_value=None)
    Attribute('gc_min_entities', default_value=None)
    Attribute('gc_max_entities', default_value=None)
    Attribute('gc_min_entities_alt', default_value=None)
        entities
    Attribute('lc_name', default_value=None)
    Attribute('lc_flavor_ref', default_value=None)
    Attribute('lc_image_ref', default_value=None)
    Attribute('lc_image_ref_alt', default_value=None)
    Attribute('sp_name', default_value=None)
    Attribute('sp_cooldown', default_value=None)
    Attribute('sp_change', default_value=None)
    Attribute('sp_policy_type', default_value=None)
    Attribute('upd_sp_change', default_value=None)
    Attribute('sp_change_percent', default_value=None)
    Attribute('sp_desired_capacity', default_value=None)
        steady state
    Attribute('lc_load_balancers', default_value=None)
    Attribute('sp_list', default_value=None)
    Attribute('wb_name', default_value=None)
    Attribute('interval_time', default_value=None)
        active servers
    Attribute('timeout', default_value=None)
        to be active
    Attribute('autoscale_endpoint_name', default_value=None)
        catalog
    Attribute('server_endpoint_name', default_value=None)
        catalog
    Attribute('server_endpoint', default_value=None)
        application
    Attribute('load_balancer_endpoint_name', default_value=None)
        service catalog
    Attribute('lbaas_region_override', default_value=None)
        catalog
    Attribute('server_region_override', default_value=None)
    Attribute('non_autoscale_username', default_value=None)
        in its service catalog
    Attribute('non_autoscale_password', default_value=None)
        in its service catalog
    Attribute('non_autoscale_tenant', default_value=None)
        its service catalog
    Attribute('autoscale_na_la_aa', default_value=None)
        gen, load balancers and autoscale
    Attribute('autoscale_na_lo_aa', default_value=None)
        and autoscale & observer role for load balancer.
    Attribute('autoscale_no_lo_aa', default_value=None)
        gen, and load balancer & admin role for autoscale.
    Attribute('autoscale_no_lo_ao', default_value=None)
        gen, load balancer & autoscale.
    Attribute('autoscale_na_la_ao', default_value=None)
        gen, load balancer & observer role for autoscale.
    Attribute('autoscale_nc_lc_aa', default_value=None)
        gen, load balancer & admin role for autoscale.
    Attribute('autoscale_nc_lc_ao', default_value=None)
        gen, load balancer & observer role for autoscale.
    Attribute('autoscale_na_la_ano', default_value=None)
        gen, load balancer & no access for autoscale.
    Attribute('autoscale_nno_lno_ao', default_value=None)
        gen, load balancer & no access for autoscale.
    Attribute('autoscale_nno_lno_aa', default_value=None)
        gen, load balancer & no access for autoscale.
    Attribute('rcv3_endpoint_name', default_value=None)
    Attribute('rcv3_load_balancer_pool', default_value=None)
    Attribute('rcv3_region_override', default_value=None)
        config file
    Attribute('rcv3_cloud_network', default_value=None)
        RackConnect

    Attribute('check_type', default_value=None)
    Attribute('check_url', default_value=None)
    Attribute('check_method', default_value=None)
    Attribute('monitoring_zones', default_value=None)
    Attribute('check_timeout', default_value=None)
    Attribute('check_period', default_value=None)
    Attribute('target_alias', default_value=None)
    Attribute('alarm_criteria', default_value=None)
    """
    def __init__(self, **kwargs):
        """
        Ignore extra parameters.
        """


@attributes(["username",
             Attribute("auth_endpoint", init_aliaser=lambda _: "endpoint"),
             Attribute("tenant_name", default_value=None),
             Attribute("api_key", default_value=None),
             Attribute("password", default_value=None),
             Attribute("strategy", default_value="keystone")])
class AuthConfig(object):
    """
    Config values for how to auth - combination of UserAuthConfig and
    UserConfig from Cloudcafe.

    :param str username: The name of the user.
    :param str tenant_name: The user's tenant_id
    :param str api_key: The user's api key, optional.
    :param str password: The user's password, optional.
    :param str endpoint: The authentication endpoint to use for the
        credentials in the [user] config section.  This value is used by the
        auth provider.
    :param str strategy: The type of authentication exposed by the
        endpoint. Currently, supported values are 'keystone', 'rax_auth'.
        """
    def __init__(self, **kwargs):
        """
        Ignore extra parameters.
        """

    def authentication_json(self):
        """
        Return the JSON necessary to authenticate this user.
        """
        if self.strategy == 'rax_auth':
            blob = {
                "auth": {
                    "RAX-KSKEY:apiKeyCredentials": {
                        "username": self.username,
                        "apiKey": self.api_key
                    }
                }
            }
        else:
            blob = {
                "auth": {
                    "passwordCredentials": {
                        "username": self.username,
                        "password": self.password
                    }
                }
            }

        if self.tenant_name:
            blob['auth']['tenantName'] = self.tenant_name

        return blob


def from_file(filename):
    """
    Read a cloudcafe config file and makes three config objects.

    :param str filename: The config file to read
    :return: `tuple` of (:class:`AuthConfig`, :class:`AutoscaleConfig`) all
        populated with the contents of the config file.
    """
    config = SafeConfigParser()
    config.read(filename)

    data = ((AuthConfig, concat([config.items('user'),
                                 config.items('user_auth_config')])),
            (AutoscaleConfig, config.items('autoscale')))

    return tuple([cls(**{i[0]: i[1] for i in items})
                  for cls, items in data])
