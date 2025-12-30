@PluginSubGroup(
    title = "Apache Flink",
    description = "This sub-group of plugins contains tasks for orchestrating Apache Flink jobs, including job submission, monitoring, and savepoint management.",
        categories = { PluginSubGroup.PluginCategory.DATA, PluginSubGroup.PluginCategory.INFRASTRUCTURE }
)
package io.kestra.plugin.flink;

import io.kestra.core.models.annotations.PluginSubGroup;