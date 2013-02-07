# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Red Hat, Inc.
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
#    under the License

from nova.api.openstack import extensions


class Multiple_create_name_template(extensions.ExtensionDescriptor):
    """Allow passing multi instance name template in Create Server v1.1 API."""

    name = "MultipleCreateNameTemplate"
    alias = "os-multiple-create-name-template"
    namespace = ("http://docs.openstack.org/compute/ext/"
                 "multiplecreatenametemplate/api/v1.1")
    updated = "2013-02-06T00:00:00+00:00"
