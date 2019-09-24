from __future__ import print_function

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import datetime


import azure.batch.batch_service_client as batch

import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

import az_config as config

def create_job_schedule(batch_client, job_schedule_id, vm_size, vm_count):
    """Creates an Azure Batch pool and job schedule with the specified ids.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_schedule_id: The id of the job schedule to create
    :param str vm_size: vm size (sku)
    :param int vm_count: number of vms to allocate
    """

    pool_info = batchmodels.PoolInformation(
        auto_pool_specification=batchmodels.AutoPoolSpecification(
            auto_pool_id_prefix="JobScheduler",
            pool=batchmodels.PoolSpecification(
                vm_size=vm_size,
                target_dedicated_nodes=vm_count,
                virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                    image_reference=batchmodels.ImageReference(
                        publisher="Canonical",
                        offer="UbuntuServer",
                        sku="18.04-LTS",
                        version="latest"
                    ),
                    node_agent_sku_id="batch.node.ubuntu 18.04"
                ),
                start_task=batchmodels.StartTask(
                    command_line="/bin/bash -c "
                                 "\"$AZ_BATCH_APP_PACKAGE_commodity_forecasting_2/DSCommodities/azure_batch/job_schedular_node_startup_tasks.sh\"",
                    wait_for_success=True,
                    user_identity=batchmodels.UserIdentity(
                        auto_user=batchmodels.AutoUserSpecification(
                            scope=batchmodels.AutoUserScope.pool,
                            elevation_level=batchmodels.ElevationLevel.admin)
                    ),
                ),
                application_package_references=[batchmodels.ApplicationPackageReference(
                    application_id="commodity_forecasting", version="2"
                )],
            ),
            keep_alive=False,
            pool_lifetime_option=batchmodels.PoolLifetimeOption.job
        )
    )

    job_spec = batchmodels.JobSpecification(
        pool_info=pool_info,
        # Terminate job once all tasks under it are complete to allow for a new
        # job to be created under the schedule
        on_all_tasks_complete=batchmodels.OnAllTasksComplete.terminate_job,
        job_manager_task=batchmodels.JobManagerTask(
            id="JobManagerTask",
            command_line="/bin/bash -c \" python3 "
                         "$AZ_BATCH_APP_PACKAGE_azure_batch_1/azure_batch/azure_batch_main.py\""
        ))


    schedule = batchmodels.Schedule(
        recurrence_interval=datetime.timedelta(days=15))

    scheduled_job = batchmodels.JobScheduleAddParameter(
        id=job_schedule_id,
        schedule=schedule,
        job_specification=job_spec)

    batch_client.job_schedule.add(cloud_job_schedule=scheduled_job)


def execute_sample():
    """Executes the sample with the specified configurations.

    :param global_config: The global configuration to use.
    :type global_config: `configparser.ConfigParser`
    :param sample_config: The sample specific configuration to use.
    :type sample_config: `configparser.ConfigParser`
    """
    # Set up the configuration
    batch_account_key = config._BATCH_ACCOUNT_KEY
    batch_account_name = config._BATCH_ACCOUNT_NAME
    batch_service_url = config._BATCH_ACCOUNT_URL




    credentials = batchauth.SharedKeyCredentials(
        batch_account_name,
        batch_account_key)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=batch_service_url)


    batch_client.config.retry_policy.retries = 5
    job_schedule_id = config._JOB_SCHEDULAR_ID

    try:
        create_job_schedule(
            batch_client,
            job_schedule_id,
            config._JOB_SCHEDULAR_POOL_VM_SIZE,
            config._JOB_SCHEDULAR_POOL_NODE_COUNT)


    except batchmodels.BatchErrorException as e:
        for x in e.error.values:
            print("BatchErrorException: ", x)


if __name__ == '__main__':
    execute_sample()