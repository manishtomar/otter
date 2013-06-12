"""
System tests for scaling policies
"""
from test_repo.autoscale.fixtures import AutoscaleFixture
from cloudcafe.common.resources import ResourcePool


class ScalingUpExecuteWebhookTest(AutoscaleFixture):

    """
    System tests to verify execute scaling policies scenarios
    """

    @classmethod
    def setUpClass(cls):
        """
        Instantiate client and configs
        """
        super(ScalingUpExecuteWebhookTest, cls).setUpClass()

    def setUp(self):
        """
        Create a scaling group with minentities over zero
        """
        self.create_group_response = self.autoscale_behaviors.create_scaling_group_given(
            gc_min_entities=self.gc_min_entities_alt)
        self.group = self.create_group_response.entity
        self.resource = ResourcePool()
        self.resource.add(self.group.id,
                          self.autoscale_client.delete_scaling_group)

    def tearDown(self):
        """
        Delete scaling group
        """
        self.resource.release()

    def test_system_execute_webhook_scale_up_change(self):
        """
        Create a scale up polic and execute its webhook and verify execution
        """
        policy_up = {'change': 1}
        execute_webhook_in_change_policy = self.autoscale_behaviors.create_policy_webhook(
            group_id=self.group.id,
            policy_data=policy_up,
            execute_webhook=True)
        self.assertEquals(execute_webhook_in_change_policy[
                          'execute_response'], 202)
        active_servers_list = self.autoscale_behaviors.wait_for_active_list_in_group_state(
            group_id=self.group.id,
            active_servers=policy_up['change'] + self.group.groupConfiguration.minEntities)
        self.assertEquals(len(active_servers_list), policy_up[
                          'change'] + self.group.groupConfiguration.minEntities)

    def test_system_execute_webhook_scale_up_change_percent(self):
        """
        Verify execution to scale up with change percent through a webhook.
        """
        policy_up = {'change_percent': 100}
        execute_webhook_in_change_percent_policy = self.autoscale_behaviors.create_policy_webhook(
            group_id=self.group.id,
            policy_data=policy_up,
            execute_webhook=True)
        self.assertEquals(execute_webhook_in_change_percent_policy[
                          'execute_response'], 202)
        servers_from_scale_up = self.autoscale_behaviors.calculate_servers(
            current=self.group.groupConfiguration.minEntities,
            percentage=policy_up['change_percent'])
        active_servers_list = self.autoscale_behaviors.wait_for_active_list_in_group_state(
            group_id=self.group.id,
            active_servers=servers_from_scale_up)
        self.assertEquals(len(active_servers_list), servers_from_scale_up)

    def test_system_execute_webhook_scale_up_desired_capacity(self):
        """
        Verify execution to scale up through a webhook, with desired capacity
        """
        desired_capacity = self.group.groupConfiguration.minEntities + 1
        policy_up = {'desired_capacity': desired_capacity}
        execute_webhook_in_desired_capacity_policy = self.autoscale_behaviors.create_policy_webhook(
            group_id=self.group.id,
            policy_data=policy_up,
            execute_webhook=True)
        self.assertEquals(execute_webhook_in_desired_capacity_policy[
                          'execute_response'], 202)
        active_servers_list = self.autoscale_behaviors.wait_for_active_list_in_group_state(
            group_id=self.group.id,
            active_servers=policy_up['desired_capacity'])
        self.assertEquals(len(active_servers_list), policy_up['desired_capacity'])
