"""
System tests for execute multiple policies
"""
from test_repo.autoscale.fixtures import AutoscaleFixture
from cloudcafe.common.resources import ResourcePool
from time import sleep


class ExecuteMultiplePoliciesTest(AutoscaleFixture):

    """
    System tests to verify execute multiple scaling policies' scenarios
    """

    @classmethod
    def setUpClass(cls):
        """
        Instantiate client and configs
        """
        super(ExecuteMultiplePoliciesTest, cls).setUpClass()

    def setUp(self):
        """
        Create a scaling group with minentities > 0, with multiple scaling policies
        and execute one scale up policy to create 2 servers
        """
        self.create_group_response = self.autoscale_behaviors.create_scaling_group_given(
            gc_min_entities=self.gc_min_entities_alt,
            gc_cooldown=0)
        self.group = self.create_group_response.entity
        print self.group.id
        self.change = 2
        self.change_percent = 50
        self.cooldown = 2
        self.policy_up_change = self.autoscale_behaviors.create_policy_given(
            group_id=self.group.id, sp_change=self.change, sp_cooldown=self.cooldown)
        self.policy_down_change = self.autoscale_behaviors.create_policy_given(
            group_id=self.group.id, sp_change=-(self.change - 1), sp_cooldown=self.cooldown)
        self.policy_up_change_percent = self.autoscale_behaviors.create_policy_given(
            group_id=self.group.id, sp_change_percent=self.change_percent, sp_cooldown=self.cooldown)
        self.policy_down_change_percent = self.autoscale_behaviors.create_policy_given(
            group_id=self.group.id, sp_change_percent=-(self.change_percent), sp_cooldown=self.cooldown)
        self.policy_desired_capacity = self.autoscale_behaviors.create_policy_given(
            group_id=self.group.id,
            sp_desired_capacity=self.group.groupConfiguration.minEntities,
            sp_cooldown=self.cooldown)
        self.policy_up_execute = {'change': self.change, 'cooldown': self.cooldown}
        self.policy_executed = self.autoscale_behaviors.create_policy_webhook(
            group_id=self.group.id,
            policy_data=self.policy_up_execute,
            execute_policy=True)
        self.resource = ResourcePool()
        self.resource.add(self.group.id,
                          self.autoscale_client.delete_scaling_group)

    def tearDown(self):
        """
        Delete scaling group
        """
        self.resource.release()

    def test_system_policy_up_cooldown(self):
        """
        Execute a scale up policy with cooldown > 0 more than once within the cooldown period,
        and verify policy execution fails when cooldown is not met
        """
        execute_on_cooldown = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_executed['policy_id'])
        self.assertEquals(execute_on_cooldown.status_code, 403,
                          msg='Scale up policy executed when cooldown is not met with %s'
                          % execute_on_cooldown.status_code)

    def test_system_policy_down_cooldown(self):
        """
        Execute a scale down policy with cooldown > 0 more than once within the cooldown period,
        and verify policy execution fails when cooldown is not met
        """
        execute_scale_down = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_down_change['id'])
        self.assertEquals(execute_scale_down.status_code, 202,
                          msg='Policy down failed to execute with %s'
                          % execute_scale_down.status_code)
        execute_on_cooldown = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_down_change['id'])
        self.assertEquals(execute_on_cooldown.status_code, 403,
                          msg='Scale down policy executed when cooldown is not met with %s'
                          % execute_on_cooldown.status_code)

    def test_system_execute_different_policies_simaltaneously(self):
        """
        Verify the policy cooldown times are not enforced when executing different policies,
        and verify all polcies result in servers as expected
        """
        execute_change_percent_scale_up = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_up_change_percent['id'])
        self.assertEquals(execute_change_percent_scale_up.status_code, 202,
                          msg='Scale up policy execution failed with %s'
                          % execute_change_percent_scale_up.status_code)
        execute_change_scale_down = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_down_change['id'])
        self.assertEquals(execute_change_scale_down.status_code, 202,
                          msg='Scale down policy execution failed with %s'
                          % execute_change_scale_down.status_code)
        execute_desired_capacity_scale = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_desired_capacity['id'])
        self.assertEquals(execute_desired_capacity_scale.status_code, 202,
                          msg='Policy with desired capacity=minentities failed to execute with %s'
                          % execute_desired_capacity_scale.status_code)
        active_servers_list = self.autoscale_behaviors.wait_for_active_list_in_group_state(
            group_id=self.group.id,
            active_servers=self.group.groupConfiguration.minEntities)
        self.assertEquals(len(
            active_servers_list), self.group.groupConfiguration.minEntities)

    def test_system_scale_up_scale_down_multiple_policies_in_sequence(self):
        """
        Verify the execution of multiple scale up and scale down policies in sequence
        after each cooldown
        """
        self._execute_policy_after_cooldown(self.group.id, self.policy_executed['policy_id'])
        self._execute_policy_after_cooldown(self.group.id, self.policy_up_change['id'])
        self._execute_policy_after_cooldown(self.group.id, self.policy_up_change_percent['id'])
        self._execute_policy_after_cooldown(self.group.id, self.policy_down_change['id'])
        self._execute_policy_after_cooldown(self.group.id, self.policy_down_change_percent['id'])
        self._execute_policy_after_cooldown(self.group.id, self.policy_desired_capacity['id'])
        active_servers_list = self.autoscale_behaviors.wait_for_active_list_in_group_state(
            group_id=self.group.id,
            active_servers=self.group.groupConfiguration.minEntities)
        self.assertEquals(len(
            active_servers_list), self.group.groupConfiguration.minEntities)

    def _execute_policy_after_cooldown(self, group_id, policy_id):
        sleep(self.cooldown)
        execute_policy = self.autoscale_client.execute_policy(
            self.group.id,
            self.policy_executed['policy_id'])
        self.assertEquals(execute_policy.status_code, 202,
                          msg='Execution of the policy after cooldown failed with %s'
                          % execute_policy.status_code)
