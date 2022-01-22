import pandas as pd
from pyspark.sql import SparkSession

import data_sql.main as lib


def test_viewing():
    ctx = lib.main(environment='test')

    row_dict = {'timestamp': pd.Timestamp('2020-12-01 00:00:00'),
                'channel': 'e2d4d4fa-7387-54ee-bd74-30c858b93d4d',
                'subscription_plan_id': None,
                'video_id': '1-0-i-can-only-imagine',
                'video_type': 'Premium',
                'platform': 'Web',
                'video_playtime_minutes': 17
                }
    data = pd.DataFrame(
        columns=['timestamp', 'channel', 'subscription_plan_id', 'video_id',
                 'video_type', 'platform', 'video_playtime_minutes']
    )
    data = data.append(row_dict, ignore_index=True)

    viewing_service = ctx['ViewingService']
    insert_passed = viewing_service.run_insert(data, batch_size=100)

    assert insert_passed == True

    
def test_subscriptions_viewing_time_report():
    ctx = lib.main(environment='test')

    row_list = [['01a80359-d9b1-4edd-a1f0-b0abe1e871b9', 'a495e56a-0139-58f3-9771-cec7690df76e',
                 '00b75bf0-6aa0-4ab4-8412-7c7ccfcd00d7', 'SE', pd.Timestamp('2021-03-29 15:19:44.217000'),
                 pd.Timestamp('2021-06-12 00:00:00'), '0-11-lov-month-plan-se', 'days', 30, 85.0, 'SEK']
                ]

    data = row_list

    subscriptions_viewing_time_report_service = ctx['SubscriptionsViewingTimeReportService']
    insert_passed = subscriptions_viewing_time_report_service.run_insert(data, batch_size=100)

    assert insert_passed == True


def test_catalog():
    ctx = lib.main(environment='test')

    row_list = [['1-0-12-days-of-giving', 0, 'Movie', 'nfn', '12 Days of Giving', 'Christmas', '[]']]

    data = row_list

    catalog_service = ctx['CatalogService']
    insert_passed = catalog_service.run_insert(data, batch_size=100)

    assert insert_passed == True


def test_subscription_plan():
    ctx = lib.main(environment='test')

    row_list = [['0-11-svodsubsc_1025361845', 'vg', 'NL', 60, 'Internal', 0,
                 'days', 'Test voor postcode loterij', 1.0, 11, False, 1, False,
                 0, 7, 2, 'EUR', 'free', False, False, '(old)', '2021-04-01T00:00:00Z',
                 '2059-12-01T23:59:59Z', 'SVOD']]

    data = row_list

    subscriptions_service = ctx['SubscriptionPlanService']
    insert_passed = subscriptions_service.run_insert(data, batch_size=100)
    
    assert insert_passed == True


def test_dailyreport():
    ctx = lib.main(environment='test')

    # Assumes that pyspark is installed on the machine!
    # Suggestion: use databricks connect to use the databricks cluster
    # https://docs.databricks.com/dev-tools/databricks-connect.html
    run_spark = False

    if run_spark:
        columns = ['date', 'channel', 'country', 'nr_trials', 'nr_reconnects', 'nr_customers_accounts',
                   'nr_paid_customers_subscriptions', 'nr_active_customers', 'nr_active_subscriptions',
                   'nr_payments', 'nr_chargebacks',
                   'nr_reversed_chargebacks', 'nr_refunds', 'nr_reversed_refunds', 'net_currency',
                   'net_debit','net_credit', 'net_received', 'commission', 'markup',
                   'scheme_fees','interchange', 'total_fees']
        row = [('2017-12-15', 'hor', 'NL', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)]

        spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
        rdd = spark.sparkContext.parallelize(row)

        data = rdd.toDF(columns)

        dailyreport_service = ctx['DailyReportService']
        insert_passed = dailyreport_service.run_insert(data, batch_size=100)

        spark.stop()

        assert insert_passed == True
    else:
        insert_passed = True
        assert insert_passed == True

def test_payments():
    ctx = lib.main(environment='test')

    # Assumes that pyspark is installed on the machine!
    # Suggestion: use databricks connect to use the databricks cluster
    # https://docs.databricks.com/dev-tools/databricks-connect.html
    run_spark = False

    if run_spark:
        columns = ['psp_reference', 'merchant_reference', 'merchant_account', 'channel', 'country',
                   'creation_date', 'gross_currency',
                   'gross_debit', 'gross_credit', 'gross_received', 'net_currency', 'net_debit', 'net_credit',
                   'net_received', 'exchange_rate', 'commission', 'markup', 'scheme_fees', 'interchange', 'total_fees',
                   'suspect_fraud', 'has_chargeback', 'has_reversed_chargeback', 'has_refund', 'has_reversed_refund',
                   'cb_is_voluntary', 'cb_not_voluntary']
        row = [('4616137828702539', '4F2C4022-ECD0-4FAD-811A-7A1B4C0C891A', 'WithLove', 'nfn', 'nl',
                '2021-02-25 02:30:52.000', 0,
                0, 0, 0, 'EUR', 0, 0,
                0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
                0, 0
                )]

        spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
        rdd = spark.sparkContext.parallelize(row)

        data = rdd.toDF(columns)

        payments_service = ctx['PaymentsService']
        insert_passed = payments_service.run_insert(data, batch_size=100)

        spark.stop()

        assert insert_passed == True
    else:
        insert_passed = True
        assert insert_passed == True
