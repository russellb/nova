#    Copyright 2013 IBM Corp.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Client side of the conductor RPC API."""

from oslo.config import cfg

from nova.objects import base as objects_base
from nova.openstack.common import jsonutils
from nova import rpcclient

CONF = cfg.CONF

rpcapi_cap_opt = cfg.StrOpt('conductor',
        help='Set a version cap for messages sent to conductor services')
CONF.register_opt(rpcapi_cap_opt, 'upgrade_levels')


class ConductorAPI(rpcclient.RpcProxy):
    """Client side of the conductor RPC API

    API version history:

    1.0 - Initial version.
    1.1 - Added migration_update
    1.2 - Added instance_get_by_uuid and instance_get_all_by_host
    1.3 - Added aggregate_host_add and aggregate_host_delete
    1.4 - Added migration_get
    1.5 - Added bw_usage_update
    1.6 - Added get_backdoor_port()
    1.7 - Added aggregate_get_by_host, aggregate_metadata_add,
          and aggregate_metadata_delete
    1.8 - Added security_group_get_by_instance and
          security_group_rule_get_by_security_group
    1.9 - Added provider_fw_rule_get_all
    1.10 - Added agent_build_get_by_triple
    1.11 - Added aggregate_get
    1.12 - Added block_device_mapping_update_or_create
    1.13 - Added block_device_mapping_get_all_by_instance
    1.14 - Added block_device_mapping_destroy
    1.15 - Added instance_get_all_by_filters and
           instance_get_all_hung_in_rebooting and
           instance_get_active_by_window
           Deprecated instance_get_all_by_host
    1.16 - Added instance_destroy
    1.17 - Added instance_info_cache_delete
    1.18 - Added instance_type_get
    1.19 - Added vol_get_usage_by_time and vol_usage_update
    1.20 - Added migration_get_unconfirmed_by_dest_compute
    1.21 - Added service_get_all_by
    1.22 - Added ping
    1.23 - Added instance_get_all
           Un-Deprecate instance_get_all_by_host
    1.24 - Added instance_get
    1.25 - Added action_event_start and action_event_finish
    1.26 - Added instance_info_cache_update
    1.27 - Added service_create
    1.28 - Added binary arg to service_get_all_by
    1.29 - Added service_destroy
    1.30 - Added migration_create
    1.31 - Added migration_get_in_progress_by_host_and_node
    1.32 - Added optional node to instance_get_all_by_host
    1.33 - Added compute_node_create and compute_node_update
    1.34 - Added service_update
    1.35 - Added instance_get_active_by_window_joined
    1.36 - Added instance_fault_create
    1.37 - Added task_log_get, task_log_begin_task, task_log_end_task
    1.38 - Added service name to instance_update
    1.39 - Added notify_usage_exists
    1.40 - Added security_groups_trigger_handler and
                 security_groups_trigger_members_refresh
           Remove instance_get_active_by_window
    1.41 - Added fixed_ip_get_by_instance, network_get,
                 instance_floating_address_get_all, quota_commit,
                 quota_rollback
    1.42 - Added get_ec2_ids, aggregate_metadata_get_by_host
    1.43 - Added compute_stop
    1.44 - Added compute_node_delete
    1.45 - Added project_id to quota_commit and quota_rollback
    1.46 - Added compute_confirm_resize
    1.47 - Added columns_to_join to instance_get_all_by_host and
                 instance_get_all_by_filters
    1.48 - Added compute_unrescue

        ... Grizzly supports message version 1.48.  So, any changes to existing
        methods in 2.x after that point should be done such that they can
        handle the version_cap being set to 1.48.

    1.49 - Added columns_to_join to instance_get_by_uuid
    1.50 - Added object_action() and object_class_action()
    1.51 - Added the 'legacy' argument to
           block_device_mapping_get_all_by_instance
    1.52 - Pass instance objects for compute_confirm_resize
    1.53 - Added compute_reboot
    1.54 - Added 'update_cells' argument to bw_usage_update
    1.55 - Pass instance objects for compute_stop
    1.56 - Remove compute_confirm_resize and
                  migration_get_unconfirmed_by_dest_compute
    1.57 - Remove migration_create()
    1.58 - Remove migration_get()

        ... Havana supports message version 1.58.  So, any changes to existing
        methods in 1.x after that point should be done such that they can
        handle the version_cap being set to 1.58.

    1.59 - Remove instance_info_cache_update()
    1.60 - Remove aggregate_metadata_add() and aggregate_metadata_delete()

    2.0 - Drop 1.x backwards compat
    """

    BASE_RPC_API_VERSION = '2.0'

    VERSION_ALIASES = {
        'grizzly': '1.48',
        'havana': '1.58',
    }

    def __init__(self):
        version_cap = self.VERSION_ALIASES.get(CONF.upgrade_levels.conductor,
                                               CONF.upgrade_levels.conductor)
        super(ConductorAPI, self).__init__(
            topic=CONF.conductor.topic,
            default_version=self.BASE_RPC_API_VERSION,
            serializer=objects_base.NovaObjectSerializer(),
            version_cap=version_cap)
        self.client = self.get_client()

    def instance_update(self, context, instance_uuid, updates,
                        service=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.38'
        updates_p = jsonutils.to_primitive(updates)
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_update',
                          instance_uuid=instance_uuid,
                          updates=updates_p,
                          service=service)

    def instance_get(self, context, instance_id):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.24'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_get', instance_id=instance_id)

    def instance_get_by_uuid(self, context, instance_uuid,
                             columns_to_join=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.49'
        cctxt = self.client.prepare(version=version)
        kwargs = {'instance_uuid': instance_uuid,
                  'columns_to_join': columns_to_join}
        return cctxt.call(context, 'instance_get_by_uuid', **kwargs)

    def migration_get_in_progress_by_host_and_node(self, context,
                                                   host, node):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.31'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context,
                          'migration_get_in_progress_by_host_and_node',
                          host=host, node=node)

    def migration_update(self, context, migration, status):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.49'
        cctxt = self.client.prepare(version=version)
        migration_p = jsonutils.to_primitive(migration)
        return cctxt.call(context, 'migration_update',
                          migration=migration_p,
                          status=status)

    def aggregate_host_add(self, context, aggregate, host):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.3'
        cctxt = self.client.prepare(version=version)
        aggregate_p = jsonutils.to_primitive(aggregate)
        return cctxt.call(context, 'aggregate_host_add',
                          aggregate=aggregate_p,
                          host=host)

    def aggregate_host_delete(self, context, aggregate, host):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.3'
        cctxt = self.client.prepare(version=version)
        aggregate_p = jsonutils.to_primitive(aggregate)
        return cctxt.call(context, 'aggregate_host_delete',
                          aggregate=aggregate_p,
                          host=host)

    def aggregate_get(self, context, aggregate_id):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.11'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'aggregate_get', aggregate_id=aggregate_id)

    def aggregate_get_by_host(self, context, host, key=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.7'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'aggregate_get_by_host', host=host, key=key)

    def aggregate_metadata_get_by_host(self, context, host, key):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.42'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'aggregate_metadata_get_by_host', host=host,
                key=key)

    def bw_usage_update(self, context, uuid, mac, start_period,
                        bw_in=None, bw_out=None,
                        last_ctr_in=None, last_ctr_out=None,
                        last_refreshed=None, update_cells=True):
        msg_kwargs = dict(uuid=uuid, mac=mac, start_period=start_period,
                          bw_in=bw_in, bw_out=bw_out, last_ctr_in=last_ctr_in,
                          last_ctr_out=last_ctr_out,
                          last_refreshed=last_refreshed,
                          update_cells=update_cells)
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.54'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'bw_usage_update', **msg_kwargs)

    def security_group_get_by_instance(self, context, instance):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.8'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context, 'security_group_get_by_instance',
                                instance=instance_p)

    def security_group_rule_get_by_security_group(self, context, secgroup):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.8'
        cctxt = self.client.prepare(version=version)
        secgroup_p = jsonutils.to_primitive(secgroup)
        return cctxt.call(context,
                'security_group_rule_get_by_security_group',
                secgroup=secgroup_p)

    def provider_fw_rule_get_all(self, context):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.9'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'provider_fw_rule_get_all')

    def agent_build_get_by_triple(self, context, hypervisor, os, architecture):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.10'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'agent_build_get_by_triple',
                hypervisor=hypervisor, os=os, architecture=architecture)

    def block_device_mapping_update_or_create(self, context, values,
                                              create=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.12'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'block_device_mapping_update_or_create',
                values=values, create=create)

    def block_device_mapping_get_all_by_instance(self, context, instance,
                                                 legacy=True):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.51'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context,
                'block_device_mapping_get_all_by_instance',
                instance=instance_p, legacy=legacy)

    def block_device_mapping_destroy(self, context, bdms=None,
                                     instance=None, volume_id=None,
                                     device_name=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.14'
        cctxt = self.client.prepare(version=version)
        bdms_p = jsonutils.to_primitive(bdms)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context, 'block_device_mapping_destroy', bdms=bdms_p,
                instance=instance_p, volume_id=volume_id,
                device_name=device_name)

    def instance_get_all_by_filters(self, context, filters, sort_key,
                                    sort_dir, columns_to_join=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.47'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_get_all_by_filters',
                filters=filters, sort_key=sort_key, sort_dir=sort_dir,
                columns_to_join=columns_to_join)

    def instance_get_active_by_window_joined(self, context, begin, end=None,
                                             project_id=None, host=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.35'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_get_active_by_window_joined',
                begin=begin, end=end, project_id=project_id, host=host)

    def instance_destroy(self, context, instance):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.16'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        cctxt.call(context, 'instance_destroy', instance=instance_p)

    def instance_info_cache_delete(self, context, instance):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.17'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        cctxt.call(context, 'instance_info_cache_delete', instance=instance_p)

    def instance_type_get(self, context, instance_type_id):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.18'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_type_get',
                instance_type_id=instance_type_id)

    def vol_get_usage_by_time(self, context, start_time):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.19'
        cctxt = self.client.prepare(version=version)
        start_time_p = jsonutils.to_primitive(start_time)
        return cctxt.call(context, 'vol_get_usage_by_time',
                start_time=start_time_p)

    def vol_usage_update(self, context, vol_id, rd_req, rd_bytes, wr_req,
                         wr_bytes, instance, update_totals=False):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.19'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context, 'vol_usage_update', vol_id=vol_id,
                rd_req=rd_req, rd_bytes=rd_bytes, wr_req=wr_req,
                wr_bytes=wr_bytes, instance=instance_p,
                update_totals=update_totals)

    def service_get_all_by(self, context, topic=None, host=None, binary=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.28'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'service_get_all_by', topic=topic,
                host=host, binary=binary)

    def instance_get_all_by_host(self, context, host, node=None,
                                 columns_to_join=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.47'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_get_all_by_host', host=host,
                node=node, columns_to_join=columns_to_join)

    def instance_fault_create(self, context, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.36'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'instance_fault_create', values=values)

    def action_event_start(self, context, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.25'
        cctxt = self.client.prepare(version=version)
        values_p = jsonutils.to_primitive(values)
        return cctxt.call(context, 'action_event_start', values=values_p)

    def action_event_finish(self, context, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.25'
        cctxt = self.client.prepare(version=version)
        values_p = jsonutils.to_primitive(values)
        return cctxt.call(context, 'action_event_finish', values=values_p)

    def service_create(self, context, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.27'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'service_create', values=values)

    def service_destroy(self, context, service_id):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.29'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'service_destroy', service_id=service_id)

    def compute_node_create(self, context, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.33'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'compute_node_create', values=values)

    def compute_node_update(self, context, node, values, prune_stats=False):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.33'
        cctxt = self.client.prepare(version=version)
        node_p = jsonutils.to_primitive(node)
        return cctxt.call(context, 'compute_node_update', node=node_p,
                values=values, prune_stats=prune_stats)

    def compute_node_delete(self, context, node):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.44'
        cctxt = self.client.prepare(version=version)
        node_p = jsonutils.to_primitive(node)
        return cctxt.call(context, 'compute_node_delete', node=node_p)

    def service_update(self, context, service, values):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.34'
        cctxt = self.client.prepare(version=version)
        service_p = jsonutils.to_primitive(service)
        return cctxt.call(context, 'service_update', service=service_p,
                values=values)

    def task_log_get(self, context, task_name, begin, end, host, state=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.37'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'task_log_get', task_name=task_name,
                begin=begin, end=end, host=host, state=state)

    def task_log_begin_task(self, context, task_name, begin, end, host,
                            task_items=None, message=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.37'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'task_log_begin_task', task_name=task_name,
                begin=begin, end=end, host=host, task_items=task_items,
                message=message)

    def task_log_end_task(self, context, task_name, begin, end, host, errors,
                          message=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.37'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'task_log_end_task', task_name=task_name,
                begin=begin, end=end, host=host, errors=errors,
                message=message)

    def notify_usage_exists(self, context, instance, current_period=False,
                            ignore_missing_network_data=True,
                            system_metadata=None, extra_usage_info=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.39'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        system_metadata_p = jsonutils.to_primitive(system_metadata)
        extra_usage_info_p = jsonutils.to_primitive(extra_usage_info)
        return cctxt.call(
            context, 'notify_usage_exists',
            instance=instance_p,
            current_period=current_period,
            ignore_missing_network_data=ignore_missing_network_data,
            system_metadata=system_metadata_p,
            extra_usage_info=extra_usage_info_p)

    def security_groups_trigger_handler(self, context, event, args):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.40'
        cctxt = self.client.prepare(version=version)
        args_p = jsonutils.to_primitive(args)
        return cctxt.call(context, 'security_groups_trigger_handler',
                event=event, args=args_p)

    def security_groups_trigger_members_refresh(self, context, group_ids):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.40'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'security_groups_trigger_members_refresh',
                group_ids=group_ids)

    def network_migrate_instance_start(self, context, instance, migration):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.41'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        migration_p = jsonutils.to_primitive(migration)
        return cctxt.call(context, 'network_migrate_instance_start',
                instance=instance_p, migration=migration_p)

    def network_migrate_instance_finish(self, context, instance, migration):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.41'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        migration_p = jsonutils.to_primitive(migration)
        return cctxt.call(context, 'network_migrate_instance_finish',
                instance=instance_p, migration=migration_p)

    def quota_commit(self, context, reservations, project_id=None,
                     user_id=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.45'
        cctxt = self.client.prepare(version=version)
        reservations_p = jsonutils.to_primitive(reservations)
        return cctxt.call(context, 'quota_commit', reservations=reservations_p,
                project_id=project_id, user_id=user_id)

    def quota_rollback(self, context, reservations, project_id=None,
                       user_id=None):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.45'
        cctxt = self.client.prepare(version=version)
        reservations_p = jsonutils.to_primitive(reservations)
        return cctxt.call(context, 'quota_rollback',
                reservations=reservations_p, project_id=project_id,
                user_id=user_id)

    def get_ec2_ids(self, context, instance):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.42'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context, 'get_ec2_ids', instance=instance_p)

    def compute_unrescue(self, context, instance):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.48'
        cctxt = self.client.prepare(version=version)
        instance_p = jsonutils.to_primitive(instance)
        return cctxt.call(context, 'compute_unrescue', instance=instance_p)

    def object_class_action(self, context, objname, objmethod, objver,
                            args, kwargs):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.50'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'object_class_action',
                          objname=objname, objmethod=objmethod,
                          objver=objver, args=args, kwargs=kwargs)

    def object_action(self, context, objinst, objmethod, args, kwargs):
        version = '2.0'
        if not self.can_send_version('2.0'):
            # NOTE(russellb) Havana compat
            version = '1.50'
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'object_action', objinst=objinst,
                          objmethod=objmethod, args=args, kwargs=kwargs)


class ComputeTaskAPI(rpcclient.RpcProxy):
    """Client side of the conductor 'compute' namespaced RPC API

    API version history:

    1.0 - Initial version (empty).
    1.1 - Added unified migrate_server call.
    1.2 - Added build_instances
    1.3 - Added unshelve_instance
    1.4 - Added reservations to migrate_server.
    1.5 - Added the leagacy_bdm parameter to build_instances
    1.6 - Made migrate_server use instance objects
    """

    BASE_RPC_API_VERSION = '1.0'
    RPC_API_NAMESPACE = 'compute_task'

    def __init__(self):
        super(ComputeTaskAPI, self).__init__(
                topic=CONF.conductor.topic,
                default_version=self.BASE_RPC_API_VERSION,
                serializer=objects_base.NovaObjectSerializer())
        self.client = self.get_client(namespace=self.RPC_API_NAMESPACE)

    def migrate_server(self, context, instance, scheduler_hint, live, rebuild,
                  flavor, block_migration, disk_over_commit,
                  reservations=None):
        if self.client.can_send_version('1.6'):
            version = '1.6'
        else:
            instance = jsonutils.to_primitive(
                    objects_base.obj_to_primitive(instance))
            version = '1.4'
        flavor_p = jsonutils.to_primitive(flavor)
        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'migrate_server',
                          instance=instance, scheduler_hint=scheduler_hint,
                          live=live, rebuild=rebuild, flavor=flavor_p,
                          block_migration=block_migration,
                          disk_over_commit=disk_over_commit,
                          reservations=reservations)

    def build_instances(self, context, instances, image, filter_properties,
            admin_password, injected_files, requested_networks,
            security_groups, block_device_mapping, legacy_bdm=True):
        instances_p = [jsonutils.to_primitive(inst) for inst in instances]
        image_p = jsonutils.to_primitive(image)
        cctxt = self.client.prepare(version='1.5')
        cctxt.cast(context, 'build_instances',
                   instances=instances_p, image=image_p,
                   filter_properties=filter_properties,
                   admin_password=admin_password,
                   injected_files=injected_files,
                   requested_networks=requested_networks,
                   security_groups=security_groups,
                   block_device_mapping=block_device_mapping,
                   legacy_bdm=legacy_bdm)

    def unshelve_instance(self, context, instance):
        cctxt = self.client.prepare(version='1.3')
        cctxt.cast(context, 'unshelve_instance', instance=instance)
