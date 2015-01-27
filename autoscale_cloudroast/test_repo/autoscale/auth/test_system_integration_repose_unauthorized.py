"""
System Integration tests autoscaling with repose
"""
from autoscale.behaviors import tags
from autoscale.client import AutoscalingAPIClient, IdentityClient
from autoscale.config import AuthConfig
from test_repo.autoscale.fixtures import AutoscaleFixture


class AutoscaleReposeUnauthTests(AutoscaleFixture):

    """
    System tests to verify repose integration with autoscale
    """

    @classmethod
    def setUpClass(cls):
        """
        Create a client for a test account that does not have the autoscale endpoint
        in its service catalog
        """
        super(AutoscaleReposeUnauthTests, cls).setUpClass()
        cls.url = cls.url.replace(cls.tenant_id, cls.non_autoscale_tenant)

        new_config = AuthConfig(username=cls.non_autoscale_username,
                                endpoint=cls.auth_config.auth_endpoint,
                                password=cls.non_autoscale_password,
                                tenant_name=cls.non_autoscale_tenant)
        identity = IdentityClient.authenticate(new_config)
        cls.autoscale_temp_client = AutoscalingAPIClient(
            url=cls.url, auth_token=identity.token, serialize_format='json',
            deserialize_format='json')

    @tags(type='repose')
    def test_system_repose_unauthorized_rate_limits_without_trailing_slash(self):
        """
        Verify the relative rate limit api without a trailing slash, when unauthorized,
        returns reponse code 403
        """
        limits_response = self.autoscale_temp_client.view_limits()
        self.assertEquals(limits_response.status_code, 403,
                          msg='Get Limits returned response code {0}'.format(
                          limits_response.status_code))

    @tags(type='repose')
    def test_system_repose_unauthorized_rate_limits_with_trailing_slash(self):
        """
        Verify the relative rate limit api with a trailing slash, when unauthorized,
        returns reponse code 403
        """
        limits_response = self.autoscale_temp_client.view_limits(self.url + '/limits/')
        self.assertEquals(limits_response.status_code, 403,
                          msg='Limits returned response code {0}'.format(
                          limits_response.status_code))

    @tags(type='repose')
    def test_system_repose_unauthorized_list_groups_on_account_without_trailing_slash(self):
        """
        Verify list scaling groups for a tenant through repose without a trailing slash,
        when unauthorized, returns response code 403
        """
        list_response = self.autoscale_temp_client.list_scaling_groups(self.url + '/groups')
        self.assertEquals(list_response.status_code, 403,
                          msg='List scaling group returned response code {0}'.format(
                          list_response.status_code))

    @tags(type='repose')
    def test_system_repose_unauthorized_list_groups_on_account_with_trailing_slash(self):
        """
        Verify list scaling groups for a tenant through repose with a trailing slash,
        when unauthorized, returns response code 403
        """
        list_response = self.autoscale_temp_client.list_scaling_groups(self.url + '/groups/')
        self.assertEquals(list_response.status_code, 403,
                          msg='List scaling group returned response code {0}'.format(
                          list_response.status_code))

    @tags(type='repose')
    def test_system_repose_unauthorized_list_groups_on_account_with_non_existant_group(self):
        """
        Verify GET non existing scaling groups through repose without a trailing slash,
        when unauthorized, returns response code 403
        """
        list_response = self.autoscale_temp_client.list_scaling_groups(self.url + '/groups/76765')
        self.assertEquals(list_response.status_code, 403,
                          msg='List scaling group returned response code {0}'.format(
                          list_response.status_code))

    @tags(type='repose')
    def test_system_repose_unauthorized_execute_webhook(self):
        """
        Verify execute webhook of a tenant by another tenant returns response code 202
        """
        group = self.autoscale_behaviors.create_scaling_group_min().entity
        policy = self.autoscale_behaviors.create_policy_webhook(group.id, {'change': 1})
        execute_wb_response = self.autoscale_temp_client.execute_webhook(policy['webhook_url'])
        self.assertEquals(execute_wb_response.status_code, 202,
                          msg='List scaling group returned response code {0}'.format(
                          execute_wb_response.status_code))
