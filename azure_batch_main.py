from __future__ import print_function

import azure.batch.batch_auth as batch_auth
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels
import time
import az_config

# Update the Batch and Storage account credential strings in az_config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def create_pool(batch_service_client, pool_id, POOL_NODE_COUNT):
    """
    Creates a pool of compute nodes with the specified OS settings.
    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size=az_config._POOL_VM_SIZE,
        target_dedicated_nodes=POOL_NODE_COUNT,
        start_task=batchmodels.StartTask(
            command_line="/bin/bash -c \"$AZ_BATCH_APP_PACKAGE_azure_batch_1/azure_batch/node_startup_tasks.sh\"",
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin)),
        ),
        application_package_references=[batchmodels.ApplicationPackageReference(
            application_id="azure_batch", version="1"
        )],
        max_tasks_per_node = 2
    )
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.
    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(batch_service_client, job_id, task_parameters):
    """
    Adds a task for each input file in the collection to the specified job.
    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    """

    print('Adding tasks to job')

    tasks = list()
    for task_parameter in task_parameters:
        command = "/bin/bash -c \"python3 $AZ_BATCH_APP_PACKAGE_azure_batch_1/azure_batch/sample.py {}\"".format(task_parameter)
        tasks.append(
            batch.models.TaskAddParameter(
                id='task {}'.format(task_parameter),
                command_line = command,
                application_package_references=[
                    batchmodels.ApplicationPackageReference(
                        application_id="azure_batch", version="1"
                    )
                ]
            )
        )
    #adds the list of tasks as a collection
    batch_service_client.task.add_collection(job_id, tasks)


def wait_for_tasks_to_complete(batch_service_client, job_id):
    """
    Returns when all tasks in the specified job reach the Completed state.
    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    """

    print("Monitoring all tasks for 'Completed' state")

    while True:
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            return True
        else:
            time.sleep(1)


if __name__ == '__main__':

    # The dict of db and data sources


    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(az_config._BATCH_ACCOUNT_NAME,
                                                  az_config._BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=az_config._BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, az_config._POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, az_config._JOB_ID, az_config._POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, az_config._JOB_ID)

        print("waiting for the tasks to complete...")
        wait_for_tasks_to_complete(batch_client,az_config.JOB_ID)

        print("Deleting the job...")
        batch_client.job.delete(az_config._JOB_ID)

        print("Deleting the pool...")
        batch_client.pool.delete(az_config._POOL_ID)

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise