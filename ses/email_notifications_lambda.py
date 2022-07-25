import re
import time
from typing import Dict, List

from common.constants.enums import EmailInformation, SortOrder, AuxPerfStatus, EmailTypes, Layers, RunTypes
from common.constants.pitt_constants import (REGION_NAME)
from common.helpers.aux_perf_helper import AuxPerfTrace, AuxPerfTraceHelper
from common.helpers.process_driver_helper import ProcessDriverHelper
from common.helpers.email_notification_helper import (EmailNotification,
                                                      EmailNotificationHelper)
from common.helpers.parameters_helper import ParametersHelper
from common.managers.cloudwatch_logs_manager import CloudWatchLogsManager

ENV = 'env'
BATCH_NO = 'batch_no'
LAYER_NAME = 'layer_name'
RUN_TYPE = 'run_type'
PROCESS_NAME = 'process_name'


def get_trace_start_time(aux_perf_helper: AuxPerfTraceHelper, batch_no: int, layer_name: str, process_name: str,
                         order_by: str, sort_order: str = SortOrder.DESCENDING) -> str:
    """
    Query aux perf trace table and get the first entry of aux_perf_trace table to get start time of the process

    Args:
        aux_perf_helper (AuxPerfTraceHelper): AuxPerfTraceHelper object
        batch_no (int): Batch number
        layer_name (str): Layer name
        process_name (str): Process name
        order_by (str): column name on which the result of the query will be ordered by.
        sort_order (str): sort order of the result of the query. Defaults to SortOrder.DESCENDING.

    Returns:
        str: Start time of the job
    """
    aux_perf_trace_data = aux_perf_helper.get_by_criteria(batch_no=batch_no,
                                                          layer_name=layer_name,
                                                          process_name=process_name,
                                                          order_by=order_by,
                                                          sort_order=sort_order
                                                          )

    beg_time = aux_perf_trace_data.beg_time

    return beg_time


def get_aux_perf_trace_data(aux_perf_helper: AuxPerfTraceHelper, batch_no: int = None, layer_name: str = None,
                            process_name: str = None) -> AuxPerfTrace:
    """
    Retrieve information from aux_perf_trace table by given criteria

    Args:
        aux_perf_helper (AuxPerfTraceHelper): AuxPerfTraceHelper object.
        batch_no (int, optional): Batch number. Defaults to None.
        layer_name (str, optional): Layer name.. Defaults to None.
        process_name (str, optional): Process name. Defaults to None.

    Returns:
        AuxPerfTrace: aux_perf_trace table object
    """

    aux_perf_trace_data = aux_perf_helper.get_all_by_criteria(batch_no=batch_no, layer_name=layer_name,
                                                              process_name=process_name)

    return aux_perf_trace_data


def get_success_orchestration_data(aux_perf_data: AuxPerfTrace, layer_name: str) -> tuple:
    """_summary_

    Args:
        aux_perf_data (AuxPerfTrace): AuxPerfTrace object

    Returns:
        tuple(int, int, int): Failed table count, Success table count, Total table count
    """
    failed_count = 0
    success_count = 0

    for aux_perf_data_row in aux_perf_data:
        if aux_perf_data_row.status == AuxPerfStatus.FAILED and aux_perf_data_row.layer_name == layer_name:
            failed_count += 1
        elif aux_perf_data_row.status == AuxPerfStatus.SUCCEED and aux_perf_data_row.layer_name == layer_name:
            success_count += 1

    total = failed_count + success_count

    return failed_count, success_count, total


def get_email_notification_data(email_notification_helper: EmailNotificationHelper,
                                email_type: str) -> EmailNotification:
    """
    Get the email subject and body meta data from email_notification table

    Args:
        email_notification_helper (EmailNotificationHelper): EmailNotificationHelper object.
        email_type (str): Email type which meta data is required. Possible Values: Failure, Success, Changes

    Returns:
        EmailNotification: EmailNotificationHelper object
    """

    email_notification_data = email_notification_helper.get_by_criteria(email_type=email_type)
    if email_notification_data == None:
        raise Exception("email_notification table object based on the input parameter is None")
    else:
        return email_notification_data


def create_email_format(email_notification_helper: EmailNotificationHelper,
                        email_type: str = None,
                        schema_name: str = None,
                        env: str = None,
                        job_name: str = None,
                        process_name: str = None,
                        layer_name: str = None,
                        batch_no: int = None,
                        total_table_count: int = None,
                        table_type_count: int = None,
                        bronze_non_priority_failed_count: int = None,
                        bronze_non_priority_success_count: int = None,
                        bronze_non_priority_total: int = None,
                        silver_non_priority_failed_count: int = None,
                        silver_non_priority_success_count: int = None,
                        silver_non_priority_total: int = None,
                        silver_build_history_failed_count: int = None,
                        silver_build_history_success_count: int = None,
                        silver_build_history_total: int = None,
                        beg_time: str = None,
                        end_time: str = None,
                        email_message: str = None
                        ) -> tuple:
    """
    Create email subject and email body

    Args:
        email_notification_data (EmailNotificationHelper): EmailNotificationHelper object
        schema_name (str, optional): schema name of the query that goes in email body. Defaults to None.
        env (str, optional): environment. Defaults to None.
        job_name (str, optional): Job name. Defaults to None.
        process_name (str, optional): Process name. Defaults to None.
        layer_name (str, optional): Layer name. Defaults to None.
        batch_no (int, optional): Batch number. Defaults to None.
        total_table_count (int, optional): Total number of tables. Defaults to None.
        table_type_count (int, optional): Count for miss matched or failed tables. Defaults to None.
        bronze_non_priority_failed_count (int, optional): Count for bronze non priority failed tables. Defaults to None.
        bronze_non_priority_total (int, optional): Count for bronze non priority total tables. Defaults to None.
        silver_non_priority_failed_count (int, optional): Count for silver non priority failed tables. Defaults to None.
        silver_non_priority_total (int, optional): Count for silver non priority total tables. Defaults to None.
        silver_build_history_failed_count (int, optional): Count for silver build history failed tables. Defaults to None.
        silver_build_history_total (int, optional): Count for silver build history total tables. Defaults to None.
        beg_time (str, optional): Process begining time. Defaults to None.
        end_time (str, optional): Process end Time. Defaults to None.
        email_message (str, optional): Additional message for email body body. Defaults to None.

    Returns:
        tuple(str, str): Email subject and Email body
    """
    email_notification_data = get_email_notification_data(email_notification_helper, email_type)
    sns_arn = email_notification_data.sns_arn

    if beg_time and end_time:
        duration = time.strftime(EmailInformation.EXECUTION_TIME_FORMAT,
                                 time.gmtime((end_time - beg_time).total_seconds()))
        beg_time = beg_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        end_time = end_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)

    if env and env.lower() == 'prod':
        env = ' '
    # email_subject = email_notification_data.email_subject
    if email_type == EmailTypes.MISSING_INPUT_PARAMETERS or email_type == EmailTypes.INVALID_INPUT_PARAMETERS or email_type == EmailTypes.NO_DATA_IN_AUX_PERF_TRACE:
        email_subject = email_notification_data.email_subject
        email_body = email_notification_data.email_body.format(email_message=email_message)

    elif email_type == EmailTypes.DMS_DDL_CHANGES or email_type == EmailTypes.DMS_TASK_ERROR:
        email_subject = email_notification_data.email_subject
        email_body = email_notification_data.email_body.format(email_message=email_message, schema_name=schema_name)

    elif email_type == EmailTypes.VALIDATION_LAYER:
        # beg_time = beg_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        # end_time = end_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)

        email_subject = email_notification_data.email_subject.format(env=env,
                                                                     job_name=job_name,
                                                                     batch_no=batch_no,
                                                                     process_name=process_name
                                                                     )
        if env == ' ':
            email_subject = email_subject.replace('|   |', '|', 1)
        email_body = email_notification_data.email_body.format(total_table_count=total_table_count,
                                                               table_type_count=table_type_count,
                                                               email_message=email_message,
                                                               process_name=process_name,
                                                               beg_time=beg_time,
                                                               end_time=end_time
                                                               )
    elif email_type == EmailTypes.ORCHESTRATION_SUCCESS_NON_PRIORITY_FAILURE:
        # duration = time.strftime(EmailInformation.EXECUTION_TIME_FORMAT, time.gmtime((end_time - beg_time).total_seconds()))
        # beg_time = beg_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        # end_time = end_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        email_subject = email_notification_data.email_subject.format(env=env,
                                                                     process_name=process_name,
                                                                     batch_no=batch_no
                                                                     )
        # if env == ' ':
        #     email_subject = email_subject.replace('|   |', '|', 1)

        email_body = email_notification_data.email_body.format(total_table_count=total_table_count,
                                                               table_type_count=table_type_count,
                                                               bronze_non_priority_failed_count=bronze_non_priority_failed_count,
                                                               bronze_non_priority_total=bronze_non_priority_total,
                                                               silver_non_priority_failed_count=silver_non_priority_failed_count,
                                                               silver_non_priority_total=silver_non_priority_total,
                                                               silver_build_history_failed_count=silver_build_history_failed_count,
                                                               silver_build_history_total=silver_build_history_total,
                                                               beg_time=beg_time,
                                                               end_time=end_time,
                                                               duration=duration
                                                               )
    elif email_type == EmailTypes.ORCHESTRATION_SUCCESSFUL:
        # duration = time.strftime(EmailInformation.EXECUTION_TIME_FORMAT, time.gmtime((end_time - beg_time).total_seconds()))
        # beg_time = beg_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        # end_time = end_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        email_subject = email_notification_data.email_subject.format(env=env,
                                                                     process_name=process_name,
                                                                     batch_no=batch_no
                                                                     )
        # if env == ' ':
        #     email_subject = email_subject.replace('|   |', '|', 1)

        email_body = email_notification_data.email_body.format(total_table_count=total_table_count,
                                                               table_type_count=table_type_count,
                                                               bronze_non_priority_success_count=bronze_non_priority_success_count,
                                                               bronze_non_priority_total=bronze_non_priority_total,
                                                               silver_non_priority_success_count=silver_non_priority_success_count,
                                                               silver_non_priority_total=silver_non_priority_total,
                                                               silver_build_history_success_count=silver_build_history_success_count,
                                                               silver_build_history_total=silver_build_history_total,
                                                               beg_time=beg_time,
                                                               end_time=end_time,
                                                               duration=duration
                                                               )


    else:
        # duration = time.strftime(EmailInformation.EXECUTION_TIME_FORMAT, time.gmtime((end_time - beg_time).total_seconds()))
        # beg_time = beg_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)
        # end_time = end_time.strftime(EmailInformation.EMAIL_DATETIME_FORMAT)

        email_subject = email_notification_data.email_subject.format(env=env,
                                                                     process_name=process_name,
                                                                     layer_name=layer_name,
                                                                     batch_no=batch_no
                                                                     )
        # if env == ' ':
        #     email_subject = email_subject.replace('|   |', '|', 1)
        email_body = email_notification_data.email_body.format(total_table_count=total_table_count,
                                                               table_type_count=table_type_count,
                                                               layer_name=layer_name,
                                                               beg_time=beg_time,
                                                               end_time=end_time,
                                                               duration=duration,
                                                               email_message=email_message,
                                                               schema_name=schema_name,
                                                               batch_no=batch_no)
    if env == ' ':
        email_subject = email_subject.replace('|   |', '|', 1)

    email_notification_helper.send_email(email_body=email_body, email_subject=email_subject, sns_topic_arn=sns_arn)
    return email_subject, email_body


def send_notification_email(email_notification_helper: EmailNotificationHelper,
                            email_type,
                            schema_name: str = None,
                            env: str = None,
                            job_name: str = None,
                            process_name: str = None,
                            layer_name: str = None,
                            batch_no: int = None,
                            total_table_count: int = None,
                            table_type_count: int = None,
                            bronze_non_priority_failed_count: int = None,
                            bronze_non_priority_success_count: int = None,
                            bronze_non_priority_total: int = None,
                            silver_non_priority_failed_count: int = None,
                            silver_non_priority_success_count: int = None,
                            silver_non_priority_total: int = None,
                            silver_build_history_failed_count: int = None,
                            silver_build_history_success_count: int = None,
                            silver_build_history_total: int = None,
                            beg_time: str = None,
                            end_time: str = None,
                            email_message: str = None):
    """
    Send email

    Args:
        email_helper (EmailHelper): email helper
        email_body (str): email body
        subject (str): email subject
        sns_arn (str, optional): SNS topic ARN. Defaults to None.
    """
    email_notification_data = get_email_notification_data(email_notification_helper, email_type)
    sns_arn = email_notification_data.sns_arn
    email_subject, email_body = create_email_format(email_notification_data=email_notification_data,
                                                    schema_name=schema_name,
                                                    env=env,
                                                    job_name=job_name,
                                                    process_name=process_name,
                                                    layer_name=layer_name,
                                                    batch_no=batch_no,
                                                    total_table_count=total_table_count,
                                                    table_type_count=table_type_count,
                                                    bronze_non_priority_failed_count=bronze_non_priority_failed_count,
                                                    bronze_non_priority_success_count=bronze_non_priority_success_count,
                                                    bronze_non_priority_total=bronze_non_priority_total,
                                                    silver_non_priority_failed_count=silver_non_priority_failed_count,
                                                    silver_non_priority_success_count=silver_non_priority_success_count,
                                                    silver_non_priority_total=silver_non_priority_total,
                                                    silver_build_history_failed_count=silver_build_history_failed_count,
                                                    silver_build_history_success_count=silver_build_history_success_count,
                                                    silver_build_history_total=silver_build_history_total,
                                                    beg_time=beg_time,
                                                    end_time=end_time,
                                                    email_message=email_message)
    # sns_arn = "arn:aws:sns:us-east-1:538628444769:pitt-test-analytics-pipeline-notification"
    email_notification_helper.send_email(email_body=email_body, email_subject=email_subject, sns_topic_arn=sns_arn)


def lambda_handler(event, context):
    parameter_helper = ParametersHelper()
    parameters = parameter_helper.parameters
    # parameters.job_name = "email_notification"
    # TODO: add email_job name in parameters
    cw_manager = CloudWatchLogsManager(parameters.log_group_name, REGION_NAME)
    aux_perf_helper = AuxPerfTraceHelper(parameters=parameters, cw_manager=cw_manager)
    email_notification_helper = EmailNotificationHelper(parameters, cw_manager)
    process_driver_helper = ProcessDriverHelper(parameters, cw_manager)
    process_driver_data = process_driver_helper.get_all_by_criteria()

    missing_input_parameters = ''
    invalid_input_parameters = ''

    # TODO: CONVERT HARD CODE INTO GLOBAL VARIABLE
    if ENV not in event.keys():
        missing_input_parameters += ' | ' + ENV + ' | '
    if BATCH_NO not in event.keys():
        missing_input_parameters += ' | ' + BATCH_NO + ' | '
    if LAYER_NAME not in event.keys():
        missing_input_parameters += ' | ' + LAYER_NAME + ' | '
    if RUN_TYPE not in event.keys():
        missing_input_parameters += ' | ' + RUN_TYPE + ' | '
    if PROCESS_NAME not in event.keys():
        missing_input_parameters += ' | ' + PROCESS_NAME + ' | '

    if missing_input_parameters:
        email_type = EmailTypes.MISSING_INPUT_PARAMETERS
        create_email_format(email_notification_helper=email_notification_helper,
                            email_type=email_type,
                            email_message=missing_input_parameters
                            )
        raise Exception("Missing input parameters")

    process_name = event[PROCESS_NAME]
    batch_no = event[BATCH_NO]
    env = event[ENV]
    layer_name = event[LAYER_NAME]
    run_type = event[RUN_TYPE]

    process_names = [process_driver_data_row.process_name for process_driver_data_row in process_driver_data]

    if process_name not in process_names:
        invalid_input_parameters += process_name + " | "

    aux_perf_trace_data = aux_perf_helper.get_all_by_criteria(batch_no=batch_no,
                                                              layer_name=layer_name,
                                                              process_name=process_name,
                                                              order_by="end_time",
                                                              sort_order=SortOrder.DESCENDING
                                                              )

    if aux_perf_trace_data == None:
        invalid_input_parameters += layer_name

    if invalid_input_parameters:
        email_type = EmailTypes.INVALID_INPUT_PARAMETERS
        create_email_format(email_notification_helper=email_notification_helper,
                            email_type=email_type,
                            email_message=invalid_input_parameters
                            )
        raise Exception("Invalid input parameters")

    if len(aux_perf_trace_data) == 0:
        email_type = EmailTypes.NO_DATA_IN_AUX_PERF_TRACE
        email_message = "No data found in aux perf trace table"
        create_email_format(email_notification_helper=email_notification_helper,
                            email_type=email_type,
                            email_message=email_message
                            )
        raise Exception("No data found in aux perf trace table")

    aux_perf_trace_data_last_row = aux_perf_trace_data[0]
    aux_perf_trace_data_first_row = aux_perf_trace_data[-1]

    end_time = aux_perf_trace_data_last_row.end_time
    job_name = aux_perf_trace_data_last_row.job_name
    beg_time = aux_perf_trace_data_first_row.beg_time
    # TODO: GET BEG_TIME AND END_TIME FROM ONLY 1 QUERY DONE

    total_table_count = len(aux_perf_trace_data)
    suceeded_table_names = []
    suceeded_table_comments = []
    failed_table_names = []
    failed_table_comments = []
    changes_table_names = []
    changes_table_comments = []
    changes_schema_name = []
    email_message_dms_changes = ''
    email_type = ''
    email_message = ''
    table_type_count = 0
    ddl_operation = []
    dms_task_error = False

    for aux_perf_trace_data_row in aux_perf_trace_data:
        if aux_perf_trace_data_row.status == AuxPerfStatus.FAILED:
            if layer_name == Layers.BRONZE_DMS:
                dms_task_error = True
                email_message += f" Failure Reason of DMS Task:  {aux_perf_trace_data_row.job_name}: " + str(
                    aux_perf_trace_data_row.comments) + " \n "
            else:
                failed_table_names.append(aux_perf_trace_data_row.table_name)
                failed_table_comments.append(aux_perf_trace_data_row.comments)
                email_message += "Table Name: " + str(aux_perf_trace_data_row.table_name) + " \n"
                email_message += f" Failure Reason of {aux_perf_trace_data_row.table_name}: " + str(
                    aux_perf_trace_data_row.comments) + " \n "

        elif aux_perf_trace_data_row.status == AuxPerfStatus.SUCCEED and aux_perf_trace_data_row.failure_type == "CHANGES":
            if layer_name == Layers.BRONZE_DMS:
                ddl_operation_reg_pat = r'(\w)+ (TABLE)|(\w)+ (table)'
                operation = re.search(ddl_operation_reg_pat, aux_perf_trace_data_row.comments).group(0)
                ddl_operation.append(operation)
                changes_schema_name.append(aux_perf_trace_data_row.owner)
                changes_table_names.append(aux_perf_trace_data_row.table_name)
                email_message_dms_changes += "DDL Operation: " + str(operation) + " \t Schema Name: " + str(
                    aux_perf_trace_data_row.owner) + " \t Table Name: " + str(
                    aux_perf_trace_data_row.table_name) + " \n "
            else:
                changes_table_names.append(aux_perf_trace_data_row.table_name)
                changes_table_comments.append(aux_perf_trace_data_row.comments)
                email_message += "Table Name: " + str(aux_perf_trace_data_row.table_name) + " \t Reason: " + str(
                    aux_perf_trace_data_row.comments) + " \n "
        else:
            suceeded_table_names.append(aux_perf_trace_data_row.table_name)
            suceeded_table_comments.append(aux_perf_trace_data_row.comments)

    if layer_name == Layers.VERIFICATION:
        email_type = EmailTypes.VALIDATION_LAYER
        table_type_count = len(failed_table_names)
        create_email_format(email_notification_helper=email_notification_helper,
                            email_type=email_type,
                            env=env,
                            job_name=job_name,
                            process_name=process_name,
                            batch_no=batch_no,
                            total_table_count=total_table_count,
                            table_type_count=table_type_count,
                            email_message=email_message,
                            beg_time=beg_time,
                            end_time=end_time
                            )

    elif layer_name == Layers.BRONZE_DMS:
        if dms_task_error:
            email_type = EmailTypes.DMS_TASK_ERROR
            create_email_format(email_notification_helper=email_notification_helper,
                                email_type=email_type,
                                email_message=email_message,
                                schema_name=parameters.ud_data_schema
                                )
        if changes_schema_name:
            email_type = EmailTypes.DMS_DDL_CHANGES
            if len(changes_schema_name) > 10:
                email_message_dms_changes = "ALERT! MORE THAN 10 DDL CHANGES HAS BEEN DETECTED THROUGH DMS \n\n"
                email_message_dms_changes += "Number of DDL changes detected : "
                email_message_dms_changes += str(len(changes_schema_name))

            create_email_format(email_notification_helper=email_notification_helper,
                                email_type=email_type,
                                email_message=email_message_dms_changes,
                                schema_name=parameters.ud_data_schema
                                )

    elif failed_table_names or changes_table_comments:
        if failed_table_names and run_type == RunTypes.INDIVIDUAL:
            email_type = EmailTypes.FAILURE
            table_type_count = len(failed_table_names)
        elif changes_table_comments and run_type == RunTypes.INDIVIDUAL:
            email_type = EmailTypes.CHANGES
            table_type_count = len(changes_table_names)
        create_email_format(email_notification_helper=email_notification_helper,
                            email_type=email_type,
                            schema_name=parameters.ud_data_schema,
                            env=env,
                            process_name=process_name,
                            layer_name=layer_name,
                            batch_no=batch_no,
                            total_table_count=total_table_count,
                            table_type_count=table_type_count,
                            beg_time=beg_time,
                            end_time=end_time,
                            email_message=email_message
                            )
    else:
        if run_type == RunTypes.ORCHESTRATION:

            gold_failed, gold_success_count, gold_total = get_success_orchestration_data(aux_perf_trace_data,
                                                                                         layer_name=layer_name)
            bronze_non_priority_failed_count, bronze_non_priority_success_count, bronze_non_priority_total = get_success_orchestration_data(
                aux_perf_trace_data, layer_name=Layers.BRONZE_NON_PRIORITY)
            silver_non_priority_failed_count, silver_non_priority_success_count, silver_non_priority_total = get_success_orchestration_data(
                aux_perf_trace_data, layer_name=Layers.SILVER_NON_PRIORITY)
            silver_build_history_failed_count, silver_build_history_success_count, silver_build_history_total = get_success_orchestration_data(
                aux_perf_trace_data, layer_name=Layers.SILVER_BUILD_HISTORY)

            if bronze_non_priority_failed_count > 0 or silver_non_priority_failed_count > 0 or silver_build_history_failed_count > 0:
                email_type = EmailTypes.ORCHESTRATION_SUCCESS_NON_PRIORITY_FAILURE

                create_email_format(email_notification_helper=email_notification_helper,
                                    email_type=email_type,
                                    env=env,
                                    process_name=process_name,
                                    batch_no=batch_no,
                                    total_table_count=total_table_count,
                                    table_type_count=table_type_count,
                                    bronze_non_priority_failed_count=bronze_non_priority_failed_count,
                                    bronze_non_priority_total=bronze_non_priority_total,
                                    silver_non_priority_failed_count=silver_non_priority_failed_count,
                                    silver_non_priority_total=silver_non_priority_total,
                                    silver_build_history_failed_count=silver_build_history_failed_count,
                                    silver_build_history_total=silver_build_history_total,
                                    beg_time=beg_time,
                                    end_time=end_time
                                    )

            else:
                email_type = EmailTypes.ORCHESTRATION_SUCCESSFUL
                create_email_format(email_notification_helper=email_notification_helper,
                                    email_type=email_type,
                                    env=env,
                                    process_name=process_name,
                                    batch_no=batch_no,
                                    total_table_count=total_table_count,
                                    table_type_count=table_type_count,
                                    bronze_non_priority_success_count=bronze_non_priority_success_count,
                                    bronze_non_priority_total=bronze_non_priority_total,
                                    silver_non_priority_success_count=silver_non_priority_success_count,
                                    silver_non_priority_total=silver_non_priority_total,
                                    silver_build_history_success_count=silver_build_history_success_count,
                                    silver_build_history_total=silver_build_history_total,
                                    beg_time=beg_time,
                                    end_time=end_time
                                    )
        elif run_type == RunTypes.INDIVIDUAL:
            email_type = EmailTypes.SUCCESS
            table_type_count = len(suceeded_table_names)
            create_email_format(email_notification_helper=email_notification_helper,
                                email_type=email_type,
                                schema_name=parameters.ud_data_schema,
                                env=env,
                                process_name=process_name,
                                layer_name=layer_name,
                                batch_no=batch_no,
                                total_table_count=total_table_count,
                                table_type_count=table_type_count,
                                beg_time=beg_time,
                                end_time=end_time
                                )