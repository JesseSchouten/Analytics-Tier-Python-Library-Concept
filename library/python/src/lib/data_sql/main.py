from contextvars import Context
import sys

from .lib.SQLAuthenticator import SQLAuthenticator
from .lib.SQLConnector import SQLConnector
from .lib.EstimateCltByCustomers import EstimateCltByCustomers
from .services.ViewingService import ViewingService
from .services.SubscriptionsViewingTimeReportService import SubscriptionsViewingTimeReportService
from .services.CatalogService import CatalogService
from .services.CustomersService import CustomersService
from .services.SubscriptionsService import SubscriptionsService
from .services.SubscriptionPlanService import SubscriptionPlanService
from .services.DailyReportService import DailyReportService
from .services.MonthReportService import MonthReportService
from .services.PaymentsService import PaymentsService
from .services.PaymentsInfoService import PaymentsInfoService
from .services.CustomerMonthStatesService import CustomerMonthStatesService
from .services.CustomerHistogramService import CustomerHistogramService
from .services.PaymentsAccountingService import PaymentsAccountingService
from .services.PaymentsReceivedService import PaymentsReceivedService
from .services.WicketlabsCustomersService import WicketlabsCustomersService
from .services.GoogleAnalyticsDailyGoalsService import GoogleAnalyticsDailyGoalsService
from .services.GoogleAnalyticsLandingPageService import GoogleAnalyticsLandingPageService
from .services.DailyAdReportService import DailyAdReportService
from .repository.ViewingRepository import ViewingRepository
from .repository.SubscriptionsViewingTimeReportRepository import SubscriptionsViewingTimeReportRepository
from .repository.CatalogRepository import CatalogRepository
from .repository.CustomersRepository import CustomersRepository
from .repository.SubscriptionsRepository import SubscriptionsRepository
from .repository.SubscriptionPlanRepository import SubscriptionPlanRepository
from .repository.DailyReportRepository import DailyReportRepository
from .repository.MonthReportRepository import MonthReportRepository
from .repository.PaymentsRepository import PaymentsRepository
from .repository.PaymentsInfoRepository import PaymentsInfoRepository
from .repository.CustomerMonthStatesRepository import CustomerMonthStatesRepository
from .repository.CustomerHistogramRepository import CustomerHistogramRepository
from .repository.PaymentsAccountingRepository import PaymentsAccountingRepository
from .repository.PaymentsReceivedRepository import PaymentsReceivedRepository
from .repository.WicketlabsCustomersRepository import WicketlabsCustomersRepository
from .repository.DailyAdReportRepository import DailyAdReportRepository
from .repository.GoogleAnalyticsDailyGoalsRepository import GoogleAnalyticsDailyGoalsRepository
from .repository.GoogleAnalyticsLandingPageRepository import GoogleAnalyticsLandingPageRepository
from .dao.test.ViewingDao import ViewingDao as ViewingDaoTEST
from .dao.test.SubscriptionsViewingTimeReportDao import SubscriptionsViewingTimeReportDao as SubscriptionsViewingTimeReportDaoTEST
from .dao.test.CatalogDao import CatalogDao as CatalogDaoTEST
from .dao.test.CustomersDao import CustomersDao as CustomersDaoTEST
from .dao.test.DailyReportDao import DailyReportDao as DailyReportDaoTEST
from .dao.test.MonthReportDao import MonthReportDao as MonthReportDaoTEST
from .dao.test.SubscriptionsDao import SubscriptionsDao as SubscriptionsDaoTEST
from .dao.test.SubscriptionPlanDao import SubscriptionPlanDao as SubscriptionPlanDaoTEST
from .dao.test.PaymentsDao import PaymentsDao as PaymentsDaoTEST
from .dao.test.PaymentsInfoDao import PaymentsInfoDao as PaymentsInfoDaoTEST
from .dao.test.CustomerMonthStatesDao import CustomerMonthStatesDao as CustomerMonthStatesDaoTEST
from .dao.test.CustomerHistogramDao import CustomerHistogramDao as CustomerHistogramDaoTEST
from .dao.test.PaymentsAccountingDao import PaymentsAccountingDao as PaymentsAccountingDaoTEST
from .dao.prd.ViewingDao import ViewingDao as ViewingDaoPRD
from .dao.prd.SubscriptionsViewingTimeReportDao import SubscriptionsViewingTimeReportDao as SubscriptionsViewingTimeReportDaoPRD
from .dao.prd.CatalogDao import CatalogDao as CatalogDaoPRD
from .dao.prd.CustomersDao import CustomersDao as CustomersDaoPRD
from .dao.prd.SubscriptionsDao import SubscriptionsDao as SubscriptionsDaoPRD
from .dao.prd.SubscriptionPlanDao import SubscriptionPlanDao as SubscriptionPlanDaoPRD
from .dao.prd.DailyReportDao import DailyReportDao as DailyReportDaoPRD
from .dao.prd.MonthReportDao import MonthReportDao as MonthReportDaoPRD
from .dao.prd.PaymentsDao import PaymentsDao as PaymentsDaoPRD
from .dao.prd.PaymentsInfoDao import PaymentsInfoDao as PaymentsInfoDaoPRD
from .dao.prd.CustomerMonthStatesDao import CustomerMonthStatesDao as CustomerMonthStatesDaoPRD
from .dao.prd.CustomerHistogramDao import CustomerHistogramDao as CustomerHistogramDaoPRD
from .dao.prd.PaymentsAccountingDao import PaymentsAccountingDao as PaymentsAccountingDaoPRD
from .dao.prd.PaymentsReceivedDao import PaymentsReceivedDao as PaymentsReceivedDaoPRD
from .dao.prd.WicketlabsCustomersDao import WicketlabsCustomersDao as WicketlabsCustomersDaoPRD
from .dao.prd.DailyAdReportDao import DailyAdReportDao as DailyAdReportDaoPRD
from .dao.prd.GoogleAnalyticsDailyGoalsDao import GoogleAnalyticsDailyGoalsDao
from .dao.prd.GoogleAnalyticsLandingPageDao import GoogleAnalyticsLandingPageDao


def main(environment='prd', dbutils=None, mock=None):
    """
    Program to manage objects to communicate with SQL server databases & corresponding tables. The context is filled
    with a set of objects as follows:
    - Authentication and SQL connector objects are created. The authentication objects use a databricks utils
    variable (;dbutils) to retrieve the keys from azure keyvault)
    Note that dbutils can only be used within a databricks notebook! Also, a SQL connector object is created
    using a custom connector to connect to the SQL tables.
    - Furthermore a service, repository and dao object (inspired from java Spring framework) are initialized and
    added to the context. To be used to communicate with the SQL tables.

    :param environment: the environment in which to run the library, either test, dev or prd.
    :param dbutils: databricks utilities object. Are not supported outside the scope of databricks notebooks. More info
    , see https://docs.databricks.com/dev-tools/databricks-utils.html

    :param environment: str
    :type dbutils: dbruntime.dbutils.DBUtils

    Insert data to a table located on the data-{environment}-sql database.
    >>> import data_sql as data_sql
    >>> ctx = lib.main(dbutils=dbutils, environment='dev')
    >>> viewingService = ctx['ViewingService']
    >>> viewingService.run_insert(df, batch_size=100)
    True
    """
    context = {}

    if environment == 'test':
        resource_name = ""
        dbutils = None
        viewing_dao = ViewingDaoTEST(context)
        subscriptions_viewing_time_report_dao = SubscriptionsViewingTimeReportDaoTEST(
            context)
        catalog_dao = CatalogDaoTEST(context)
        customers_dao = CustomersDaoTEST(context)
        subscriptions_dao = SubscriptionsDaoTEST(context)
        subscription_plan_dao = SubscriptionPlanDaoTEST(context)
        daily_report_dao = DailyReportDaoTEST(context)
        month_report_dao = MonthReportDaoTEST(context)
        payments_dao = PaymentsDaoTEST(context)
        payments_info_dao = PaymentsInfoDaoTEST(context)
        customer_month_states_dao = CustomerMonthStatesDaoTEST(context)
        customer_histogram_dao = CustomerHistogramDaoTEST(context)
        payments_accounting_dao = PaymentsAccountingDaoTEST(context)
        wicketlabs_customers_doa = WicketlabsCustomersDaoPRD(context)
        daily_ad_report_dao = DailyAdReportDaoPRD(context)
        payments_received_dao = mock
        google_analytics_daily_goals_dao = mock
        google_analytics_landing_page_dao = mock if mock != None else GoogleAnalyticsLandingPageDao(
            context)
    elif environment == 'dev':
        resource_name = "data-dev-sql"
        dbutils = dbutils
        viewing_dao = ViewingDaoPRD(context)
        subscriptions_viewing_time_report_dao = SubscriptionsViewingTimeReportDaoPRD(
            context)
        catalog_dao = CatalogDaoPRD(context)
        customers_dao = CustomersDaoPRD(context)
        subscriptions_dao = SubscriptionsDaoPRD(context)
        subscription_plan_dao = SubscriptionPlanDaoPRD(context)
        daily_report_dao = DailyReportDaoPRD(context)
        month_report_dao = MonthReportDaoPRD(context)
        payments_dao = PaymentsDaoPRD(context)
        payments_info_dao = PaymentsInfoDaoPRD(context)
        customer_month_states_dao = CustomerMonthStatesDaoPRD(context)
        customer_histogram_dao = CustomerHistogramDaoPRD(context)
        payments_accounting_dao = PaymentsAccountingDaoPRD(context)
        payments_received_dao = PaymentsReceivedDaoPRD(context)
        wicketlabs_customers_doa = WicketlabsCustomersDaoPRD(context)
        daily_ad_report_dao = DailyAdReportDaoPRD(context)
        google_analytics_daily_goals_dao = GoogleAnalyticsDailyGoalsDao(
            context)
        google_analytics_landing_page_dao = GoogleAnalyticsLandingPageDao(
            context)
    elif environment == 'prd':
        resource_name = "data-prd-sql"
        dbutils = dbutils
        viewing_dao = ViewingDaoPRD(context)
        subscriptions_viewing_time_report_dao = SubscriptionsViewingTimeReportDaoPRD(
            context)
        catalog_dao = CatalogDaoPRD(context)
        customers_dao = CustomersDaoPRD(context)
        subscriptions_dao = SubscriptionsDaoPRD(context)
        subscription_plan_dao = SubscriptionPlanDaoPRD(context)
        daily_report_dao = DailyReportDaoPRD(context)
        month_report_dao = MonthReportDaoPRD(context)
        payments_dao = PaymentsDaoPRD(context)
        payments_info_dao = PaymentsInfoDaoPRD(context)
        customer_month_states_dao = CustomerMonthStatesDaoPRD(context)
        customer_histogram_dao = CustomerHistogramDaoPRD(context)
        payments_accounting_dao = PaymentsAccountingDaoPRD(context)
        payments_received_dao = PaymentsReceivedDaoPRD(context)
        wicketlabs_customers_doa = WicketlabsCustomersDaoPRD(context)
        daily_ad_report_dao = DailyAdReportDaoPRD(context)
        google_analytics_daily_goals_dao = GoogleAnalyticsDailyGoalsDao(
            context)
        google_analytics_landing_page_dao = GoogleAnalyticsLandingPageDao(
            context)
    else:
        sys.exit("Please enter a valid environment name, either prd, dev or test!")
        
    context['SQLAuthenticator'] = SQLAuthenticator(resource_name, dbutils=dbutils)
    context['SQLConnector'] = SQLConnector(context)
    context['EstimateCltByCustomers'] = EstimateCltByCustomers(context)
    context['ViewingDao'] = viewing_dao
    context['SubscriptionsViewingTimeReportDao'] = subscriptions_viewing_time_report_dao
    context['CatalogDao'] = catalog_dao
    context['CustomersDao'] = customers_dao
    context['SubscriptionsDao'] = subscriptions_dao
    context['SubscriptionPlanDao'] = subscription_plan_dao
    context['DailyReportDao'] = daily_report_dao
    context['MonthReportDao'] = month_report_dao
    context['PaymentsDao'] = payments_dao
    context['WicketlabsCustomersDao'] = wicketlabs_customers_doa
    context['PaymentsInfoDao'] = payments_info_dao
    context['CustomerMonthStatesDao'] = customer_month_states_dao
    context['CustomerHistogramDao'] = customer_histogram_dao
    context['PaymentsAccountingDao'] = payments_accounting_dao
    context['PaymentsReceivedDao'] = payments_received_dao
    context['DailyAdReportDao'] = daily_ad_report_dao
    context['GoogleAnalyticsDailyGoalsDao'] = google_analytics_daily_goals_dao
    context['GoogleAnalyticsLandingPageDao'] = google_analytics_landing_page_dao
    context['ViewingRepository'] = ViewingRepository(context)
    context['SubscriptionsViewingTimeReportRepository'] = SubscriptionsViewingTimeReportRepository(context)
    context['CatalogRepository'] = CatalogRepository(context)
    context['CustomersRepository'] = CustomersRepository(context)
    context['SubscriptionsRepository'] = SubscriptionsRepository(context)
    context['SubscriptionPlanRepository'] = SubscriptionPlanRepository(context)
    context['DailyReportRepository'] = DailyReportRepository(context)
    context['MonthReportRepository'] = MonthReportRepository(context)
    context['PaymentsRepository'] = PaymentsRepository(context)
    context['PaymentsReceivedRepository'] = PaymentsReceivedRepository(context)
    context['PaymentsInfoRepository'] = PaymentsInfoRepository(context)
    context['CustomerMonthStatesRepository'] = CustomerMonthStatesRepository(context)
    context['CustomerHistogramRepository'] = CustomerHistogramRepository(context)
    context['PaymentsAccountingRepository'] = PaymentsAccountingRepository(context)
    context['WicketlabsCustomersRepository'] = WicketlabsCustomersRepository(context)
    context['ViewingService'] = ViewingService(context)
    context['SubscriptionsViewingTimeReportService'] = SubscriptionsViewingTimeReportService(context)
    context['DailyAdReportRepository'] = DailyAdReportRepository(context)
    context['GoogleAnalyticsDailyGoalsRepository'] = GoogleAnalyticsDailyGoalsRepository(
        context)
    context['GoogleAnalyticsLandingPageRepository'] = GoogleAnalyticsLandingPageRepository(
        context)
    context['CatalogService'] = CatalogService(context)
    context['CustomersService'] = CustomersService(context)
    context['SubscriptionsService'] = SubscriptionsService(context)
    context['SubscriptionPlanService'] = SubscriptionPlanService(context)
    context['DailyReportService'] = DailyReportService(context)
    context['MonthReportService'] = MonthReportService(context)
    context['PaymentsService'] = PaymentsService(context)
    context['PaymentsReceivedService'] = PaymentsReceivedService(context)
    context['PaymentsInfoService'] = PaymentsInfoService(context)
    context['CustomerMonthStatesService'] = CustomerMonthStatesService(context)
    context['CustomerHistogramService'] = CustomerHistogramService(context)
    context['PaymentsAccountingService'] = PaymentsAccountingService(context)
    context['WicketlabsCustomersService'] = WicketlabsCustomersService(context)
    context['DailyAdReportService'] = DailyAdReportService(context)
    context['GoogleAnalyticsDailyGoalsService'] = GoogleAnalyticsDailyGoalsService(
        context)
    context['GoogleAnalyticsLandingPageService'] = GoogleAnalyticsLandingPageService(
        context)

    return context
