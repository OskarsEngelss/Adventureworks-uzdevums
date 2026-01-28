import pendulum
import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import cross_downstream

# --- DIMENSIONS (Levels 1-3) ---
L1_DAGS = [
    "load_dim_date",
    "extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks",
    "extract_transform_combine_aging_tier_data_into_dimagingtier_and_upload_to_starrocks",
    "extract_transform_combine_customer_segment_data_into_dimcustomersegment_and_upload_to_starrocks",
    "extract_transform_combine_feedback_category_data_into_dimfeedbackcategory_and_upload_to_starrocks",
    "extract_transform_combine_finance_category_data_into_dimfinancecategory_and_upload_to_starrocks",
    "extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks",
    "extract_transform_combine_return_reason_data_into_dimreturnreason_and_upload_to_starrocks",
]

L2_DAGS = [
    "extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks",
    "extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks",
    "extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks",
]

L3_DAGS = [
    "extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks",
    "extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks",
    "extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks",
    "extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks",
    "extract_transform_combine_vendor_data_into_dimvendor_and_upload_to_starrocks",
]

# --- FACTS (Level 4) ---
L4_DAGS = [
    "extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks",
    "extract_transform_load_inventory_data_into_factinventory_and_upload_to_starrocks",
    "extract_transform_load_finance_data_into_factfinance_and_upload_to_starrocks",
    "extract_transform_load_production_data_into_factproduction_and_upload_to_starrocks",
    "extract_transform_load_purchases_data_into_factpurchases_and_upload_to_starrocks",
    "extract_transform_load_sales_data_into_factemployeesales_and_upload_to_starrocks",
    "extract_transform_load_returns_data_into_factreturns_and_upload_to_starrocks",
    "extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks",
    "extract_transform_load_promotion_data_into_factpromotionresponse_and_upload_to_starrocks",
]

# --- AGGREGATES (Level 5) ---
L5_DAGS = [
    "update_aggregates_daily_sales",
    "update_aggregates_daily_inventory",
    "update_aggregates_weekly_sales",
    "update_aggregates_monthly_sales",
    "update_regional_sales_aggregation",
    "update_agg_monthly_product_performance",
]

with DAG(
    dag_id="master_adventureworks_orchestrator",
    schedule="0 4 * * *", 
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["orchestration", "adventureworks", "full_load"],
) as dag:

    def create_trigger(target_dag_id):
        return TriggerDagRunOperator(
            task_id=f"run_{target_dag_id[:100]}",
            trigger_dag_id=target_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
            failed_states=["failed"]
        )

    # Instantiate all triggers
    l1_tasks = [create_trigger(d) for d in L1_DAGS]
    l2_tasks = [create_trigger(d) for d in L2_DAGS]
    l3_tasks = [create_trigger(d) for d in L3_DAGS]
    l4_tasks = [create_trigger(d) for d in L4_DAGS]
    l5_tasks = [create_trigger(d) for d in L5_DAGS]

    # --- PARALLEL PIPELINE CHAINING ---
    # cross_downstream allows all tasks in the first list to finish 
    # before any task in the second list starts.
    
    cross_downstream(l1_tasks, l2_tasks)
    cross_downstream(l2_tasks, l3_tasks)
    cross_downstream(l3_tasks, l4_tasks)
    cross_downstream(l4_tasks, l5_tasks)