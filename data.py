import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os

fake = Faker()

# NOTE: the small mapping tables below are written directly in this file (same style as the Uber project).
# The 3 big ones (stores, sales reps, corporate accounts) are loaded from ./mappings/*.json
# because they have hundreds of rows. The same JSON files are used for the initial load.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_mapping(file_name):
    with open(os.path.join(BASE_DIR, 'Data', file_name), encoding='utf-8') as f:
        return json.load(f)

OPERATOR_MAPPING = [
    {'operator_id': 1, 'operator': 'Vodafone Egypt', 'operator_ar': 'فودافون', 'prefix': '010', 'wallet_name': 'Vodafone Cash', 'ussd_code': '*9#'},
    {'operator_id': 2, 'operator': 'Etisalat by e&', 'operator_ar': 'اتصالات', 'prefix': '011', 'wallet_name': 'Etisalat Cash', 'ussd_code': '*555#'},
    {'operator_id': 3, 'operator': 'Orange Egypt', 'operator_ar': 'أورنج', 'prefix': '012', 'wallet_name': 'Orange Cash', 'ussd_code': '#100#'},
    {'operator_id': 4, 'operator': 'WE (Telecom Egypt)', 'operator_ar': 'وي', 'prefix': '015', 'wallet_name': 'WE Pay', 'ussd_code': '*688#'}
]

GOVERNORATE_MAPPING = [
    {'governorate_id': 1, 'governorate': 'Cairo', 'governorate_ar': 'القاهرة', 'region': 'Greater Cairo', 'latitude': 30.0444, 'longitude': 31.2357},
    {'governorate_id': 2, 'governorate': 'Giza', 'governorate_ar': 'الجيزة', 'region': 'Greater Cairo', 'latitude': 30.0131, 'longitude': 31.2089},
    {'governorate_id': 3, 'governorate': 'Qalyubia', 'governorate_ar': 'القليوبية', 'region': 'Greater Cairo', 'latitude': 30.4103, 'longitude': 31.2078},
    {'governorate_id': 4, 'governorate': 'Alexandria', 'governorate_ar': 'الإسكندرية', 'region': 'Alexandria & Delta', 'latitude': 31.2001, 'longitude': 29.9187},
    {'governorate_id': 5, 'governorate': 'Beheira', 'governorate_ar': 'البحيرة', 'region': 'Alexandria & Delta', 'latitude': 31.0341, 'longitude': 30.4682},
    {'governorate_id': 6, 'governorate': 'Gharbia', 'governorate_ar': 'الغربية', 'region': 'Alexandria & Delta', 'latitude': 30.8754, 'longitude': 31.0335},
    {'governorate_id': 7, 'governorate': 'Dakahlia', 'governorate_ar': 'الدقهلية', 'region': 'Alexandria & Delta', 'latitude': 31.0409, 'longitude': 31.3785},
    {'governorate_id': 8, 'governorate': 'Sharqia', 'governorate_ar': 'الشرقية', 'region': 'Alexandria & Delta', 'latitude': 30.5877, 'longitude': 31.502},
    {'governorate_id': 9, 'governorate': 'Monufia', 'governorate_ar': 'المنوفية', 'region': 'Alexandria & Delta', 'latitude': 30.5972, 'longitude': 30.9876},
    {'governorate_id': 10, 'governorate': 'Kafr El Sheikh', 'governorate_ar': 'كفر الشيخ', 'region': 'Alexandria & Delta', 'latitude': 31.3085, 'longitude': 30.9399},
    {'governorate_id': 11, 'governorate': 'Damietta', 'governorate_ar': 'دمياط', 'region': 'Alexandria & Delta', 'latitude': 31.4165, 'longitude': 31.8133},
    {'governorate_id': 12, 'governorate': 'Port Said', 'governorate_ar': 'بورسعيد', 'region': 'Canal & Sinai', 'latitude': 31.2653, 'longitude': 32.3019},
    {'governorate_id': 13, 'governorate': 'Ismailia', 'governorate_ar': 'الإسماعيلية', 'region': 'Canal & Sinai', 'latitude': 30.5965, 'longitude': 32.2715},
    {'governorate_id': 14, 'governorate': 'Suez', 'governorate_ar': 'السويس', 'region': 'Canal & Sinai', 'latitude': 29.9668, 'longitude': 32.5498},
    {'governorate_id': 15, 'governorate': 'North Sinai', 'governorate_ar': 'شمال سيناء', 'region': 'Canal & Sinai', 'latitude': 31.1316, 'longitude': 33.7984},
    {'governorate_id': 16, 'governorate': 'South Sinai', 'governorate_ar': 'جنوب سيناء', 'region': 'Canal & Sinai', 'latitude': 28.2362, 'longitude': 33.6253},
    {'governorate_id': 17, 'governorate': 'Fayoum', 'governorate_ar': 'الفيوم', 'region': 'Upper Egypt', 'latitude': 29.3084, 'longitude': 30.8428},
    {'governorate_id': 18, 'governorate': 'Beni Suef', 'governorate_ar': 'بني سويف', 'region': 'Upper Egypt', 'latitude': 29.0661, 'longitude': 31.0994},
    {'governorate_id': 19, 'governorate': 'Minya', 'governorate_ar': 'المنيا', 'region': 'Upper Egypt', 'latitude': 28.1099, 'longitude': 30.7503},
    {'governorate_id': 20, 'governorate': 'Assiut', 'governorate_ar': 'أسيوط', 'region': 'Upper Egypt', 'latitude': 27.1809, 'longitude': 31.1837},
    {'governorate_id': 21, 'governorate': 'Sohag', 'governorate_ar': 'سوهاج', 'region': 'Upper Egypt', 'latitude': 26.5591, 'longitude': 31.6957},
    {'governorate_id': 22, 'governorate': 'Qena', 'governorate_ar': 'قنا', 'region': 'Upper Egypt', 'latitude': 26.1551, 'longitude': 32.716},
    {'governorate_id': 23, 'governorate': 'Luxor', 'governorate_ar': 'الأقصر', 'region': 'Upper Egypt', 'latitude': 25.6872, 'longitude': 32.6396},
    {'governorate_id': 24, 'governorate': 'Aswan', 'governorate_ar': 'أسوان', 'region': 'Upper Egypt', 'latitude': 24.0889, 'longitude': 32.8998},
    {'governorate_id': 25, 'governorate': 'Red Sea', 'governorate_ar': 'البحر الأحمر', 'region': 'Red Sea & Desert', 'latitude': 27.2579, 'longitude': 33.8116},
    {'governorate_id': 26, 'governorate': 'Matrouh', 'governorate_ar': 'مطروح', 'region': 'Red Sea & Desert', 'latitude': 31.3543, 'longitude': 27.2373},
    {'governorate_id': 27, 'governorate': 'New Valley', 'governorate_ar': 'الوادي الجديد', 'region': 'Red Sea & Desert', 'latitude': 25.4519, 'longitude': 30.5464}
]

PLAN_MAPPING = [
    {'plan_id': 1, 'operator_id': 1, 'plan_name': 'Vodafone Flex 70', 'plan_type': 'Prepaid', 'price_egp': 70, 'data_gb': 10, 'voice_minutes': 500, 'sms_count': 100, 'validity_days': 30},
    {'plan_id': 2, 'operator_id': 1, 'plan_name': 'Vodafone Flex 150', 'plan_type': 'Prepaid', 'price_egp': 150, 'data_gb': 30, 'voice_minutes': 1500, 'sms_count': 300, 'validity_days': 30},
    {'plan_id': 3, 'operator_id': 1, 'plan_name': 'Vodafone Red 400', 'plan_type': 'Postpaid', 'price_egp': 400, 'data_gb': 60, 'voice_minutes': 3000, 'sms_count': 1000, 'validity_days': 30},
    {'plan_id': 4, 'operator_id': 1, 'plan_name': 'Vodafone Net Only 100', 'plan_type': 'Data-only', 'price_egp': 100, 'data_gb': 50, 'voice_minutes': 0, 'sms_count': 0, 'validity_days': 30},
    {'plan_id': 5, 'operator_id': 2, 'plan_name': 'Etisalat Fakka 60', 'plan_type': 'Prepaid', 'price_egp': 60, 'data_gb': 8, 'voice_minutes': 400, 'sms_count': 100, 'validity_days': 30},
    {'plan_id': 6, 'operator_id': 2, 'plan_name': 'Etisalat Super Kix 140', 'plan_type': 'Prepaid', 'price_egp': 140, 'data_gb': 28, 'voice_minutes': 1400, 'sms_count': 300, 'validity_days': 30},
    {'plan_id': 7, 'operator_id': 2, 'plan_name': 'Etisalat Hekaya 350', 'plan_type': 'Postpaid', 'price_egp': 350, 'data_gb': 55, 'voice_minutes': 2500, 'sms_count': 800, 'validity_days': 30},
    {'plan_id': 8, 'operator_id': 2, 'plan_name': 'Etisalat Home Net 120', 'plan_type': 'Data-only', 'price_egp': 120, 'data_gb': 60, 'voice_minutes': 0, 'sms_count': 0, 'validity_days': 30},
    {'plan_id': 9, 'operator_id': 3, 'plan_name': 'Orange Go 65', 'plan_type': 'Prepaid', 'price_egp': 65, 'data_gb': 9, 'voice_minutes': 450, 'sms_count': 100, 'validity_days': 30},
    {'plan_id': 10, 'operator_id': 3, 'plan_name': 'Orange Shabab 130', 'plan_type': 'Prepaid', 'price_egp': 130, 'data_gb': 25, 'voice_minutes': 1200, 'sms_count': 250, 'validity_days': 30},
    {'plan_id': 11, 'operator_id': 3, 'plan_name': 'Orange Dream 380', 'plan_type': 'Postpaid', 'price_egp': 380, 'data_gb': 58, 'voice_minutes': 2800, 'sms_count': 900, 'validity_days': 30},
    {'plan_id': 12, 'operator_id': 3, 'plan_name': 'Orange Net 110', 'plan_type': 'Data-only', 'price_egp': 110, 'data_gb': 55, 'voice_minutes': 0, 'sms_count': 0, 'validity_days': 30},
    {'plan_id': 13, 'operator_id': 4, 'plan_name': 'WE Mix 55', 'plan_type': 'Prepaid', 'price_egp': 55, 'data_gb': 7, 'voice_minutes': 350, 'sms_count': 80, 'validity_days': 30},
    {'plan_id': 14, 'operator_id': 4, 'plan_name': 'WE Super 120', 'plan_type': 'Prepaid', 'price_egp': 120, 'data_gb': 22, 'voice_minutes': 1000, 'sms_count': 200, 'validity_days': 30},
    {'plan_id': 15, 'operator_id': 4, 'plan_name': 'WE Gold 300', 'plan_type': 'Postpaid', 'price_egp': 300, 'data_gb': 50, 'voice_minutes': 2200, 'sms_count': 700, 'validity_days': 30},
    {'plan_id': 16, 'operator_id': 4, 'plan_name': 'WE Home Internet 200', 'plan_type': 'Data-only', 'price_egp': 200, 'data_gb': 140, 'voice_minutes': 0, 'sms_count': 0, 'validity_days': 30}
]

OFFER_MAPPING = [
    {'offer_id': 1, 'offer_name': 'No offer', 'offer_type': 'None', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 2, 'offer_name': 'Double Balance Weekend', 'offer_type': 'Balance bonus', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 3, 'offer_name': 'Ramadan Bonus Data', 'offer_type': 'Bonus data', 'discount_pct': 0, 'bonus_data_gb': 5, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 4, 'offer_name': 'Cashback 10%', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 10, 'operator_id': None},
    {'offer_id': 5, 'offer_name': 'Student Discount 15%', 'offer_type': 'Discount', 'discount_pct': 15, 'bonus_data_gb': 0, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 6, 'offer_name': 'Eid Mega Offer', 'offer_type': 'Bonus data', 'discount_pct': 0, 'bonus_data_gb': 10, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 7, 'offer_name': 'First Recharge Gift', 'offer_type': 'Bonus data', 'discount_pct': 0, 'bonus_data_gb': 2, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 8, 'offer_name': 'Wallet Cashback 20 EGP', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 20, 'operator_id': None},
    {'offer_id': 9, 'offer_name': 'Loyalty Points 2x', 'offer_type': 'Loyalty', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 10, 'offer_name': 'Black Friday 30% Off', 'offer_type': 'Discount', 'discount_pct': 30, 'bonus_data_gb': 0, 'cashback_egp': 0, 'operator_id': None},
    {'offer_id': 11, 'offer_name': 'Vodafone Cash Fridays', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 15, 'operator_id': 1},
    {'offer_id': 12, 'offer_name': 'Etisalat Cash Rewards', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 15, 'operator_id': 2},
    {'offer_id': 13, 'offer_name': 'Orange Cash Bonus', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 15, 'operator_id': 3},
    {'offer_id': 14, 'offer_name': 'WE Pay Cashback', 'offer_type': 'Cashback', 'discount_pct': 0, 'bonus_data_gb': 0, 'cashback_egp': 15, 'operator_id': 4}
]

SERVICE_TYPE_MAPPING = [
    {'service_type_id': 1, 'service_type': 'Voice Call', 'unit': 'second'},
    {'service_type_id': 2, 'service_type': 'Mobile Data', 'unit': 'MB'},
    {'service_type_id': 3, 'service_type': 'SMS', 'unit': 'message'},
    {'service_type_id': 4, 'service_type': 'International Roaming', 'unit': 'MB'},
    {'service_type_id': 5, 'service_type': 'Value Added Service', 'unit': 'subscription'}
]

PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'Vodafone Cash', 'is_wallet': True, 'is_card': False, 'requires_auth': True},
    {'payment_method_id': 2, 'payment_method': 'Etisalat Cash', 'is_wallet': True, 'is_card': False, 'requires_auth': True},
    {'payment_method_id': 3, 'payment_method': 'Orange Cash', 'is_wallet': True, 'is_card': False, 'requires_auth': True},
    {'payment_method_id': 4, 'payment_method': 'WE Pay', 'is_wallet': True, 'is_card': False, 'requires_auth': True},
    {'payment_method_id': 5, 'payment_method': 'InstaPay', 'is_wallet': False, 'is_card': False, 'requires_auth': True},
    {'payment_method_id': 6, 'payment_method': 'Fawry', 'is_wallet': False, 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 7, 'payment_method': 'Credit Card', 'is_wallet': False, 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 8, 'payment_method': 'Debit Card', 'is_wallet': False, 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 9, 'payment_method': 'Scratch Card', 'is_wallet': False, 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 10, 'payment_method': 'Cash (Retail Store)', 'is_wallet': False, 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 11, 'payment_method': 'Bank Transfer / Invoice', 'is_wallet': False, 'is_card': False, 'requires_auth': True}
]

TRANSACTION_STATUS_MAPPING = [
    {'status_id': 1, 'status': 'Success', 'is_successful': True},
    {'status_id': 2, 'status': 'Failed', 'is_successful': False},
    {'status_id': 3, 'status': 'Pending', 'is_successful': False}
]

FAILURE_REASON_MAPPING = [
    {'failure_reason_id': 1, 'failure_reason': None},
    {'failure_reason_id': 2, 'failure_reason': 'Insufficient balance'},
    {'failure_reason_id': 3, 'failure_reason': 'Network error'},
    {'failure_reason_id': 4, 'failure_reason': 'Invalid number'},
    {'failure_reason_id': 5, 'failure_reason': 'Invalid or used voucher'},
    {'failure_reason_id': 6, 'failure_reason': 'Payment declined'},
    {'failure_reason_id': 7, 'failure_reason': 'Wallet limit exceeded'},
    {'failure_reason_id': 8, 'failure_reason': 'Timeout'},
    {'failure_reason_id': 9, 'failure_reason': 'Credit check failed'},
    {'failure_reason_id': 10, 'failure_reason': 'Incomplete KYC documents'},
    {'failure_reason_id': 11, 'failure_reason': 'Number not eligible for port-in'},
    {'failure_reason_id': 12, 'failure_reason': 'Out of stock'}
]

WALLET_TXN_TYPE_MAPPING = [
    {'wallet_txn_type_id': 1, 'wallet_txn_type': 'Send Money', 'charges_fee': True},
    {'wallet_txn_type_id': 2, 'wallet_txn_type': 'Cash In', 'charges_fee': False},
    {'wallet_txn_type_id': 3, 'wallet_txn_type': 'Cash Out (ATM)', 'charges_fee': True},
    {'wallet_txn_type_id': 4, 'wallet_txn_type': 'Bill Payment', 'charges_fee': True},
    {'wallet_txn_type_id': 5, 'wallet_txn_type': 'Merchant Payment (QR)', 'charges_fee': False},
    {'wallet_txn_type_id': 6, 'wallet_txn_type': 'Airtime Top-up', 'charges_fee': False}
]

CHANNEL_MAPPING = [
    {'channel_id': 1, 'channel': 'Mobile App'},
    {'channel_id': 2, 'channel': 'USSD'},
    {'channel_id': 3, 'channel': 'Retail Store'},
    {'channel_id': 4, 'channel': 'Fawry Kiosk'},
    {'channel_id': 5, 'channel': 'Website'},
    {'channel_id': 6, 'channel': 'Call Center'}
]

SEGMENT_MAPPING = [
    {'segment_id': 1, 'segment': 'Individual'},
    {'segment_id': 2, 'segment': 'Youth'},
    {'segment_id': 3, 'segment': 'Corporate'},
    {'segment_id': 4, 'segment': 'VIP'}
]

DEVICE_MAPPING = [
    {'device_id': 1, 'brand': 'Samsung', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 2, 'brand': 'Oppo', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 3, 'brand': 'Xiaomi', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 4, 'brand': 'Apple iPhone', 'device_type': 'Smartphone', 'os': 'iOS'},
    {'device_id': 5, 'brand': 'Realme', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 6, 'brand': 'Infinix', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 7, 'brand': 'Huawei', 'device_type': 'Smartphone', 'os': 'Android'},
    {'device_id': 8, 'brand': 'Nokia', 'device_type': 'Feature Phone', 'os': 'KaiOS'},
    {'device_id': 9, 'brand': 'Huawei 4G Router', 'device_type': 'Router', 'os': 'Embedded'}
]

NETWORK_TYPE_MAPPING = [
    {'network_type_id': 1, 'network_type': '3G'},
    {'network_type_id': 2, 'network_type': '4G'},
    {'network_type_id': 3, 'network_type': '4.5G'},
    {'network_type_id': 4, 'network_type': '5G'}
]

LOYALTY_TIER_MAPPING = [
    {'loyalty_tier_id': 1, 'loyalty_tier': 'Bronze', 'min_points': 0},
    {'loyalty_tier_id': 2, 'loyalty_tier': 'Silver', 'min_points': 2000},
    {'loyalty_tier_id': 3, 'loyalty_tier': 'Gold', 'min_points': 6000},
    {'loyalty_tier_id': 4, 'loyalty_tier': 'Platinum', 'min_points': 15000}
]

CONTRACT_TYPE_MAPPING = [
    {'contract_type_id': 1, 'contract_type': 'Prepaid (No Contract)', 'duration_months': 0, 'is_corporate': False},
    {'contract_type_id': 2, 'contract_type': 'Postpaid 12M', 'duration_months': 12, 'is_corporate': False},
    {'contract_type_id': 3, 'contract_type': 'Postpaid 24M', 'duration_months': 24, 'is_corporate': False},
    {'contract_type_id': 4, 'contract_type': 'Corporate Pool 12M', 'duration_months': 12, 'is_corporate': True},
    {'contract_type_id': 5, 'contract_type': 'Corporate Pool 24M', 'duration_months': 24, 'is_corporate': True},
    {'contract_type_id': 6, 'contract_type': 'Corporate Fleet 36M', 'duration_months': 36, 'is_corporate': True}
]

PAYMENT_TERMS_MAPPING = [
    {'payment_terms_id': 1, 'payment_terms': 'Prepaid / Immediate', 'days': 0},
    {'payment_terms_id': 2, 'payment_terms': 'Net 15', 'days': 15},
    {'payment_terms_id': 3, 'payment_terms': 'Net 30', 'days': 30},
    {'payment_terms_id': 4, 'payment_terms': 'Net 60', 'days': 60},
    {'payment_terms_id': 5, 'payment_terms': 'Monthly Direct Debit', 'days': 30}
]

ORDER_STATUS_MAPPING = [
    {'order_status_id': 1, 'order_status': 'Draft', 'is_final': False},
    {'order_status_id': 2, 'order_status': 'Submitted', 'is_final': False},
    {'order_status_id': 3, 'order_status': 'Credit Check', 'is_final': False},
    {'order_status_id': 4, 'order_status': 'Approved', 'is_final': False},
    {'order_status_id': 5, 'order_status': 'Provisioned', 'is_final': True},
    {'order_status_id': 6, 'order_status': 'Rejected', 'is_final': True},
    {'order_status_id': 7, 'order_status': 'Cancelled', 'is_final': True}
]

SALES_CHANNEL_MAPPING = [
    {'sales_channel_id': 1, 'sales_channel': 'Own Store', 'is_b2b': False},
    {'sales_channel_id': 2, 'sales_channel': 'Authorized Dealer', 'is_b2b': False},
    {'sales_channel_id': 3, 'sales_channel': 'Online Shop', 'is_b2b': False},
    {'sales_channel_id': 4, 'sales_channel': 'Telesales', 'is_b2b': False},
    {'sales_channel_id': 5, 'sales_channel': 'Field Sales (B2B)', 'is_b2b': True}
]

SALE_TYPE_MAPPING = [
    {'sale_type_id': 1, 'sale_type': 'New Line', 'requires_sim': True},
    {'sale_type_id': 2, 'sale_type': 'MNP Port-in', 'requires_sim': True},
    {'sale_type_id': 3, 'sale_type': 'SIM Replacement', 'requires_sim': True},
    {'sale_type_id': 4, 'sale_type': 'Device Sale', 'requires_sim': False},
    {'sale_type_id': 5, 'sale_type': 'Upgrade (Device + Plan)', 'requires_sim': False},
    {'sale_type_id': 6, 'sale_type': 'VIP Number', 'requires_sim': True}
]

PRODUCT_MAPPING = [
    {'product_id': 1, 'product_name': 'Prepaid SIM Card', 'product_category': 'SIM', 'unit_price_egp': 15, 'unit_cost_egp': 5},
    {'product_id': 2, 'product_name': 'Postpaid SIM Card', 'product_category': 'SIM', 'unit_price_egp': 0, 'unit_cost_egp': 3},
    {'product_id': 3, 'product_name': 'eSIM Activation', 'product_category': 'SIM', 'unit_price_egp': 30, 'unit_cost_egp': 2},
    {'product_id': 4, 'product_name': 'SIM Replacement', 'product_category': 'SIM', 'unit_price_egp': 25, 'unit_cost_egp': 3},
    {'product_id': 5, 'product_name': 'VIP / Golden Number', 'product_category': 'Number', 'unit_price_egp': 500, 'unit_cost_egp': 20},
    {'product_id': 6, 'product_name': 'Smartphone - Entry', 'product_category': 'Device', 'unit_price_egp': 6000, 'unit_cost_egp': 5000},
    {'product_id': 7, 'product_name': 'Smartphone - Mid', 'product_category': 'Device', 'unit_price_egp': 14000, 'unit_cost_egp': 12200},
    {'product_id': 8, 'product_name': 'Smartphone - Premium', 'product_category': 'Device', 'unit_price_egp': 38000, 'unit_cost_egp': 34000},
    {'product_id': 9, 'product_name': '4G Router', 'product_category': 'Device', 'unit_price_egp': 1500, 'unit_cost_egp': 1200},
    {'product_id': 10, 'product_name': '5G Home Router', 'product_category': 'Device', 'unit_price_egp': 3200, 'unit_cost_egp': 2700},
    {'product_id': 11, 'product_name': 'MiFi 4G Pocket', 'product_category': 'Device', 'unit_price_egp': 1100, 'unit_cost_egp': 900},
    {'product_id': 12, 'product_name': 'Tablet', 'product_category': 'Device', 'unit_price_egp': 7500, 'unit_cost_egp': 6500},
    {'product_id': 13, 'product_name': 'Accessory Bundle', 'product_category': 'Accessory', 'unit_price_egp': 350, 'unit_cost_egp': 150}
]

INDUSTRY_MAPPING = [
    {'industry_id': 1, 'industry': 'Banking & Finance'},
    {'industry_id': 2, 'industry': 'Retail'},
    {'industry_id': 3, 'industry': 'Healthcare'},
    {'industry_id': 4, 'industry': 'Education'},
    {'industry_id': 5, 'industry': 'Manufacturing'},
    {'industry_id': 6, 'industry': 'Oil & Gas'},
    {'industry_id': 7, 'industry': 'Government'},
    {'industry_id': 8, 'industry': 'Hospitality & Tourism'},
    {'industry_id': 9, 'industry': 'Logistics & Transport'},
    {'industry_id': 10, 'industry': 'IT & Software'},
    {'industry_id': 11, 'industry': 'Real Estate & Construction'},
    {'industry_id': 12, 'industry': 'Media & Advertising'}
]

# Big mapping tables (loaded from JSON)
STORE_MAPPING = load_mapping('map_stores.json')
SALES_REP_MAPPING = load_mapping('map_sales_reps.json')
CORPORATE_ACCOUNT_MAPPING = load_mapping('map_corporate_accounts.json')

# ---------- Lookups (name -> id, id -> row) ----------
OPERATOR_BY_ID = {o['operator_id']: o for o in OPERATOR_MAPPING}
GOV_BY_ID = {g['governorate_id']: g for g in GOVERNORATE_MAPPING}
PLAN_BY_ID = {p['plan_id']: p for p in PLAN_MAPPING}
OFFER_BY_ID = {o['offer_id']: o for o in OFFER_MAPPING}
PRODUCT_BY_NAME = {p['product_name']: p for p in PRODUCT_MAPPING}
CONTRACT_BY_ID = {c['contract_type_id']: c for c in CONTRACT_TYPE_MAPPING}
REP_BY_ID = {r['sales_rep_id']: r for r in SALES_REP_MAPPING}

PLANS_BY_OPERATOR = {}
POSTPAID_BY_OPERATOR = {}
for p in PLAN_MAPPING:
    PLANS_BY_OPERATOR.setdefault(p['operator_id'], []).append(p)
    if p['plan_type'] == 'Postpaid':
        POSTPAID_BY_OPERATOR.setdefault(p['operator_id'], []).append(p)

REPS_BY_STORE = {}
for r in SALES_REP_MAPPING:
    REPS_BY_STORE.setdefault(r['store_id'], []).append(r)

STORES_BY_OP_CHANNEL = {}
for s in STORE_MAPPING:
    STORES_BY_OP_CHANNEL.setdefault((s['operator_id'], s['sales_channel_id']), []).append(s)

OPERATOR_IDS = [1, 2, 3, 4]
OPERATOR_WEIGHTS = [40, 28, 22, 10]          # approx. market share: Vodafone, Etisalat, Orange, WE
BIG_GOVS = {'Cairo': 8, 'Giza': 6, 'Alexandria': 5, 'Qalyubia': 3, 'Dakahlia': 3, 'Sharqia': 3}
GOV_WEIGHTS = [BIG_GOVS.get(g['governorate'], 1) for g in GOVERNORATE_MAPPING]
VAT_RATE = 0.14

FIRST_NAMES = ['Ahmed', 'Mohamed', 'Mahmoud', 'Omar', 'Youssef', 'Ali', 'Hassan', 'Khaled', 'Mostafa', 'Ibrahim',
               'Karim', 'Tarek', 'Amr', 'Hossam', 'Fatma', 'Mariam', 'Nour', 'Salma', 'Aya', 'Heba', 'Menna',
               'Rana', 'Yasmin', 'Dina', 'Mona', 'Sara', 'Reem', 'Esraa', 'Hagar', 'Nada']
FEMALE_NAMES = {'Fatma', 'Mariam', 'Nour', 'Salma', 'Aya', 'Heba', 'Menna', 'Rana', 'Yasmin', 'Dina', 'Mona',
                'Sara', 'Reem', 'Esraa', 'Hagar', 'Nada'}
LAST_NAMES = ['Hassan', 'Ibrahim', 'Salah', 'Abdelrahman', 'Mostafa', 'Ahmed', 'Mahmoud', 'ElSayed', 'Fathy', 'Nabil',
              'Gamal', 'Farouk', 'Samir', 'Hamdy', 'Shaker', 'Zaki', 'Abdallah', 'Kamel', 'Soliman', 'Metwally',
              'ElShazly', 'Badawy']
MERCHANTS = ['Carrefour', 'Spinneys', 'Talabat', 'Uber Egypt', 'Noon', 'Vezeeta', 'Breadfast', 'Metro Markets',
             'El Araby', 'Cilantro', 'Gourmet Egypt', 'Fawry Pay', 'Rabbit Mart', 'Seoudi']
BILLS = ['Electricity', 'Water', 'Gas', 'Landline', 'Internet (ADSL)', 'Traffic Fines', 'University Fees']

EVENT_TYPES = ['usage', 'recharge', 'wallet_transaction', 'subscription', 'sale', 'corporate_order']
EVENT_WEIGHTS = [34, 20, 15, 8, 16, 7]


# ---------- Fixed customer base (so the same customer shows up again and again -> proper dim_customer) ----------
def build_customer_pool(size=1000):
    rng = random.Random(42)
    pool = []
    used_numbers = set()
    accounts_by_op = {}
    for a in CORPORATE_ACCOUNT_MAPPING:
        accounts_by_op.setdefault(a['primary_operator_id'], []).append(a)

    for _ in range(size):
        operator_id = rng.choices(OPERATOR_IDS, OPERATOR_WEIGHTS)[0]
        corp = rng.choice(accounts_by_op[operator_id]) if rng.random() < 0.15 else None   # 15% are company lines

        while True:
            number = f"{OPERATOR_BY_ID[operator_id]['prefix']}{rng.randint(0, 99999999):08d}"
            if number not in used_numbers:
                used_numbers.add(number)
                break

        governorate_id = corp['governorate_id'] if corp else rng.choices(GOVERNORATE_MAPPING, GOV_WEIGHTS)[0]['governorate_id']
        plan = rng.choice(POSTPAID_BY_OPERATOR[operator_id]) if corp else rng.choice(PLANS_BY_OPERATOR[operator_id])
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        birth_year = rng.randint(1960, 2008)
        age = 2026 - birth_year
        points = int(rng.expovariate(1 / 3500))
        device = rng.choice(DEVICE_MAPPING)

        if plan['plan_type'] == 'Prepaid':
            contract_type_id = 1
        elif corp:
            contract_type_id = corp['contract_type_id']
        else:
            contract_type_id = rng.choice([2, 3])

        pool.append({
            'customer_id': str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            'customer_type': 'Corporate Line' if corp else 'Individual',
            'corporate_account_id': corp['corporate_account_id'] if corp else None,
            'customer_name': f'{first} {last}',
            'email': f'{first}.{last}{rng.randint(1, 999)}@example.com'.lower(),
            'gender': 'F' if first in FEMALE_NAMES else 'M',
            'birth_year': birth_year,
            'age_group': '18-24' if age < 25 else '25-34' if age < 35 else '35-44' if age < 45 else '45-59' if age < 60 else '60+',
            'language': rng.choices(['ar', 'en'], [88, 12])[0],
            'kyc_status': rng.choices(['Verified', 'Pending', 'Rejected'], [92, 6, 2])[0],
            'msisdn': '+20' + number[1:],
            'imsi': '602' + {1: '02', 2: '03', 3: '01', 4: '04'}[operator_id] + f'{rng.randint(0, 9999999999):010d}',
            'iccid': f'8920{rng.randint(0, 10**15 - 1):015d}',
            'imei': str(rng.randint(10**14, 10**15 - 1)),
            'operator_id': operator_id,
            'governorate_id': governorate_id,
            'plan_id': plan['plan_id'],
            'contract_type_id': contract_type_id,
            'segment_id': 3 if corp else rng.choices([1, 2, 4], [70, 25, 5])[0],
            'device_id': device['device_id'],
            'is_5g_capable': device['device_id'] in (1, 2, 3, 4, 5) and rng.random() < 0.5,
            'loyalty_points': points,
            'loyalty_tier_id': 4 if points >= 15000 else 3 if points >= 6000 else 2 if points >= 2000 else 1,
            'arpu_egp': round(plan['price_egp'] * rng.uniform(0.7, 1.6), 2),
            'churn_risk_base': round(rng.betavariate(2, 8), 3),
            'activation_date': (datetime(2026, 11, 7) - timedelta(days=rng.randint(30, 3650))).date().isoformat(),
        })
    return pool


CUSTOMERS = build_customer_pool()


# ---------- Customers CHANGE over time (this is what makes SCD Type 2 meaningful) ----------
# ~35% of the customers get 1-3 changes (new plan, new device, loyalty upgrade, KYC verified, moved city)
# at random moments in the last 28 days. An event always carries the customer attributes that were valid
# AT THE EVENT TIME (see customer_at), so old/bulk events show the old values and live events the new ones.
BUILD_TIME = datetime.now()


def tier_for(points):
    return 4 if points >= 15000 else 3 if points >= 6000 else 2 if points >= 2000 else 1


def add_customer_history():
    rng = random.Random(7)
    for c in CUSTOMERS:
        c['versions'] = []
        if rng.random() >= 0.35:
            continue
        state = dict(c)
        days_ago = sorted(rng.sample(range(1, 29), rng.choices([1, 2, 3], [60, 30, 10])[0]), reverse=True)  # oldest first
        for d in days_ago:
            options = ['plan', 'device', 'loyalty']
            if state['kyc_status'] != 'Verified':
                options.append('kyc')
            if not c['corporate_account_id']:
                options.append('move')
            kind = rng.choice(options)
            changes = {}
            if kind == 'plan':
                pool = [p for p in PLANS_BY_OPERATOR[c['operator_id']] if p['plan_type'] != 'Prepaid'] if c['corporate_account_id'] else PLANS_BY_OPERATOR[c['operator_id']]
                new_plan = rng.choice([p for p in pool if p['plan_id'] != state['plan_id']])
                changes['plan_id'] = new_plan['plan_id']
                changes['arpu_egp'] = round(new_plan['price_egp'] * rng.uniform(0.7, 1.6), 2)
                if not c['corporate_account_id']:
                    changes['contract_type_id'] = 1 if new_plan['plan_type'] == 'Prepaid' else rng.choice([2, 3])
            elif kind == 'device':
                new_device = rng.choice([x for x in DEVICE_MAPPING if x['device_id'] != state['device_id']])
                changes['device_id'] = new_device['device_id']
                changes['imei'] = str(rng.randint(10**14, 10**15 - 1))
                changes['is_5g_capable'] = new_device['device_id'] in (1, 2, 3, 4, 5) and rng.random() < 0.6
            elif kind == 'loyalty':
                points = state['loyalty_points'] + rng.randint(1500, 8000)
                changes['loyalty_points'] = points
                changes['loyalty_tier_id'] = tier_for(points)
            elif kind == 'kyc':
                changes['kyc_status'] = 'Verified'
            else:  # move
                changes['governorate_id'] = rng.choice([g['governorate_id'] for g in GOVERNORATE_MAPPING
                                                        if g['governorate_id'] != state['governorate_id']])
            state.update(changes)
            valid_from = BUILD_TIME - timedelta(days=d, hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            c['versions'].append({'valid_from': valid_from, 'changes': changes})


add_customer_history()


def customer_at(customer, ts):
    """Customer attributes as they were valid at time ts."""
    state = dict(customer)
    for v in customer['versions']:
        if v['valid_from'] <= ts:
            state.update(v['changes'])
    return state


# ---------- Small helpers ----------
def random_msisdn():
    operator_id = random.choices(OPERATOR_IDS, OPERATOR_WEIGHTS)[0]
    return f"+20{OPERATOR_BY_ID[operator_id]['prefix'][1:]}{random.randint(0, 99999999):08d}"


def pick_offer(operator_id, probability=0.35):
    if random.random() > probability:
        return 1  # No offer
    eligible = [o for o in OFFER_MAPPING if o['offer_id'] != 1 and o['operator_id'] in (None, operator_id)]
    return random.choice(eligible)['offer_id']


def pick_status(allowed_failure_ids, fail_rate=0.08, pending_rate=0.02):
    """Returns (status_id, failure_reason_id). 1=Success, 2=Failed, 3=Pending. failure_reason_id 1 = no failure."""
    r = random.random()
    if r < fail_rate:
        return 2, random.choice(allowed_failure_ids)
    if r < fail_rate + pending_rate:
        return 3, 8   # Pending -> Timeout
    return 1, 1


def generate_telecom_event(event_type=None, days_back=0):
    """Generate ONE telecom event.
    event_type : usage / recharge / wallet_transaction / subscription / sale / corporate_order (random if None)
    days_back  : 0 -> timestamp is "now" (live API), N -> random time in the last N days (bulk / initial load)

    Only the fields that belong to the event type are added (no empty columns) - the Spark schema fills the rest with null.
    """
    if event_type is None:
        event_type = random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]

    # Generate timestamp
    now = datetime.now()
    if days_back:
        event_time = now - timedelta(days=random.randint(0, days_back), hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    else:
        event_time = now

    # Pick the customer (or the company, for B2B orders)
    account = random.choice(CORPORATE_ACCOUNT_MAPPING) if event_type == 'corporate_order' else None
    if account:
        # A company is its own customer (no personal fields)
        customer = {
            'customer_id': f"CORP-{account['corporate_account_id']:04d}", 'customer_type': 'Corporate Account',
            'corporate_account_id': account['corporate_account_id'], 'customer_name': account['company_name'],
            'email': None, 'gender': None, 'birth_year': None, 'age_group': None, 'language': None,
            'kyc_status': None, 'msisdn': account['contact_msisdn'], 'imsi': None, 'iccid': None, 'imei': None,
            'activation_date': account['contract_start_date'], 'is_5g_capable': None, 'loyalty_tier_id': None,
            'loyalty_points': None, 'arpu_egp': None, 'churn_risk_base': None, 'segment_id': 3, 'plan_id': None,
            'contract_type_id': account['contract_type_id'], 'device_id': None,
            'operator_id': account['primary_operator_id'], 'governorate_id': account['governorate_id'],
        }
    else:
        customer = customer_at(random.choice(CUSTOMERS), event_time)   # attributes valid at event time

    operator_id = customer['operator_id']
    governorate = GOV_BY_ID[customer['governorate_id']]
    plan = PLAN_BY_ID.get(customer['plan_id'])
    offer_id = pick_offer(operator_id)
    offer = OFFER_BY_ID[offer_id]
    tenure_months = max(1, (event_time.date() - datetime.fromisoformat(customer['activation_date']).date()).days // 30)
    fraud_score = round(random.betavariate(1, 30), 3)

    # Common fields (every event has these)
    event = {
        # Keys/Identifiers
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'event_timestamp': event_time.isoformat(),
        'session_id': str(uuid.uuid4()),
        'source_system': 'telecom-api',
        'api_version': 'v2',
        'processing_latency_ms': random.randint(20, 1500),

        # Customer Information
        'customer_id': customer['customer_id'],
        'customer_type': customer['customer_type'],
        'corporate_account_id': customer['corporate_account_id'],
        'customer_name': customer['customer_name'],
        'customer_email': customer['email'],
        'gender': customer['gender'],
        'birth_year': customer['birth_year'],
        'age_group': customer['age_group'],
        'language': customer['language'],
        'kyc_status': customer['kyc_status'],
        'msisdn': customer['msisdn'],
        'imsi': customer['imsi'],
        'iccid': customer['iccid'],
        'imei': customer['imei'],
        'activation_date': customer['activation_date'],
        'is_5g_capable': customer['is_5g_capable'],
        'tenure_months': tenure_months,
        'loyalty_tier_id': customer['loyalty_tier_id'],
        'loyalty_points': customer['loyalty_points'],
        'arpu_egp': customer['arpu_egp'],
        'churn_risk_score': None if customer['churn_risk_base'] is None else round(min(1.0, customer['churn_risk_base'] + random.uniform(-0.03, 0.05)), 3),

        # Foreign Keys to Mapping Tables
        'segment_id': customer['segment_id'],
        'operator_id': operator_id,
        'plan_id': customer['plan_id'],
        'contract_type_id': customer['contract_type_id'],
        'governorate_id': governorate['governorate_id'],
        'device_id': customer['device_id'],
        'channel_id': random.choices([1, 2, 3, 4, 5, 6], [40, 25, 10, 15, 7, 3])[0],
        'network_type_id': random.choices([1, 2, 3, 4], [10, 45, 40, 5])[0],
        'payment_method_id': random.choices([operator_id, 5, 6, 7, 8, 9, 10], [30, 10, 20, 10, 8, 12, 10])[0],
        'offer_id': offer_id,
        'status_id': 1,
        'failure_reason_id': 1,

        # Location (around the governorate center -> always inside Egypt)
        'cell_tower_id': f"CT-{governorate['governorate_id']:02d}-{random.randint(1, 900):04d}",
        'latitude': round(governorate['latitude'] + random.uniform(-0.15, 0.15), 6),
        'longitude': round(governorate['longitude'] + random.uniform(-0.15, 0.15), 6),

        # Risk
        'fraud_score': fraud_score,
        'is_fraud_suspected': fraud_score > 0.12,
    }

    # ======================= RECHARGE =======================
    if event_type == 'recharge':
        event['status_id'], event['failure_reason_id'] = pick_status([2, 3, 4, 5, 6, 8])
        success = event['status_id'] == 1
        amount = random.choice([5, 10, 15, 20, 25, 30, 50, 100, 200, 500])
        event.update({
            'recharge_amount_egp': amount,
            'vat_egp': round(amount * VAT_RATE, 2),
            'bonus_balance_egp': float(amount) if offer['offer_name'] == 'Double Balance Weekend' and success else 0.0,
            'voucher_code': fake.bothify('#### #### #### ####') if event['payment_method_id'] == 9 else None,
            'cashback_egp': float(offer['cashback_egp']) if success else 0.0,
            'balance_after_egp': round(random.uniform(0, 400) + (amount if success else 0), 2),
        })

    # ======================= USAGE =======================
    elif event_type == 'usage':
        event['status_id'], event['failure_reason_id'] = pick_status([3, 8], 0.02, 0.0)
        service = random.choices(SERVICE_TYPE_MAPPING, [30, 45, 10, 3, 12])[0]
        service_type_id = service['service_type_id']
        is_roaming = service_type_id == 4
        event['service_type_id'] = service_type_id
        event['is_roaming'] = is_roaming

        if service_type_id == 1:        # Voice call
            duration = random.choice([random.randint(5, 60), random.randint(60, 600), random.randint(600, 3600)])
            event.update({'call_duration_sec': duration, 'destination_msisdn': random_msisdn(),
                          'usage_charge_egp': round(duration / 60 * 0.45, 2)})
        elif service_type_id in (2, 4): # Data / roaming
            mb = round(random.uniform(1, 2000), 2)
            event.update({'data_mb': mb, 'usage_charge_egp': round(mb / 1024 * (60 if is_roaming else 6), 2)})
        elif service_type_id == 3:      # SMS
            count = random.randint(1, 5)
            event.update({'sms_count': count, 'destination_msisdn': random_msisdn(), 'usage_charge_egp': round(count * 0.3, 2)})
        else:                           # Value added service
            event['usage_charge_egp'] = random.choice([1.0, 2.5, 5.0, 10.0])
        event['balance_after_egp'] = round(random.uniform(0, 300), 2)

    # ======================= WALLET (Vodafone Cash / Etisalat Cash / Orange Cash / WE Pay) =======================
    elif event_type == 'wallet_transaction':
        event['status_id'], event['failure_reason_id'] = pick_status([2, 3, 4, 6, 7, 8])
        success = event['status_id'] == 1
        txn = random.choices(WALLET_TXN_TYPE_MAPPING, [30, 18, 8, 14, 20, 10])[0]
        txn_type_id = txn['wallet_txn_type_id']
        amount = round(random.choice([20, 50, 100, 200, 500, 1000, 2500, 5000]) * random.uniform(0.5, 1.5), 2)
        event.update({
            'wallet_txn_type_id': txn_type_id,
            'wallet_amount_egp': amount,
            'wallet_fee_egp': round(max(1.0, amount * 0.005), 2) if txn['charges_fee'] else 0.0,
            'counterparty_msisdn': random_msisdn() if txn_type_id == 1 else None,
            'merchant_name': random.choice(MERCHANTS) if txn_type_id == 5 else random.choice(BILLS) if txn_type_id == 4 else None,
            'wallet_balance_after_egp': round(random.uniform(0, 20000), 2),
            'cashback_egp': round(amount * 0.01, 2) if txn_type_id == 5 and success else 0.0,
            'payment_method_id': operator_id,   # the wallet of the customer's own operator
        })

    # ======================= SUBSCRIPTION (plans) =======================
    elif event_type == 'subscription':
        event['status_id'], event['failure_reason_id'] = pick_status([2, 3, 6, 8])
        action = random.choices(['New', 'Renew', 'Upgrade', 'Downgrade', 'Cancel'], [20, 50, 12, 8, 10])[0]
        target_plan = plan if action == 'Renew' else random.choice(PLANS_BY_OPERATOR[operator_id])
        event.update({
            'subscribed_plan_id': target_plan['plan_id'],
            'subscription_action': action,
            'subscription_price_egp': 0.0 if action == 'Cancel' else float(target_plan['price_egp']),
            'discount_egp': round(target_plan['price_egp'] * offer['discount_pct'] / 100, 2),
            'auto_renew': random.random() < 0.6 and action != 'Cancel',
            'subscription_start_date': event_time.date().isoformat(),
            'subscription_end_date': (event_time + timedelta(days=target_plan['validity_days'])).date().isoformat(),
        })

    # ======================= SALE (stores / dealers / online / telesales) =======================
    elif event_type == 'sale':
        sale = random.choices(SALE_TYPE_MAPPING, [38, 10, 8, 24, 12, 8])[0]
        sale_type_id = sale['sale_type_id']
        failure_ids = [6, 10, 12] + ([11] if sale_type_id == 2 else [])
        event['status_id'], event['failure_reason_id'] = pick_status(failure_ids, 0.06, 0.02)
        success = event['status_id'] == 1

        products_by_sale_type = {
            1: ['Prepaid SIM Card', 'Postpaid SIM Card', 'eSIM Activation'],
            2: ['Prepaid SIM Card', 'Postpaid SIM Card', 'eSIM Activation'],
            3: ['SIM Replacement'],
            4: ['Smartphone - Entry', 'Smartphone - Mid', 'Smartphone - Premium', '4G Router', '5G Home Router',
                'MiFi 4G Pocket', 'Tablet', 'Accessory Bundle'],
            5: ['Smartphone - Entry', 'Smartphone - Mid', 'Smartphone - Premium', '5G Home Router'],
            6: ['VIP / Golden Number'],
        }
        product = PRODUCT_BY_NAME[random.choice(products_by_sale_type[sale_type_id])]
        quantity = random.choice([1, 1, 1, 2, 3]) if product['product_category'] == 'Accessory' else 1
        is_device = product['product_category'] == 'Device'

        # Which store sold it? (physical stores: prefer the customer's governorate)
        sales_channel_id = random.choices([1, 2, 3, 4], [45, 30, 18, 7])[0]
        stores = STORES_BY_OP_CHANNEL.get((operator_id, sales_channel_id), [])
        if sales_channel_id in (1, 2):
            stores = [s for s in stores if s['governorate_id'] == governorate['governorate_id']] or stores
        store = random.choice(stores)
        rep = random.choice(REPS_BY_STORE[store['store_id']])

        # Pricing
        gross = round(product['unit_price_egp'] * quantity, 2)
        discount = round(gross * offer['discount_pct'] / 100, 2)
        net_before_vat = round(gross - discount, 2)
        vat = round(net_before_vat * VAT_RATE, 2)
        total = round(net_before_vat + vat, 2)
        cost = round(product['unit_cost_egp'] * quantity, 2)
        margin = round(net_before_vat - cost, 2)

        # Installments (expensive devices only)
        installment_months = random.choice([0, 0, 6, 12, 24]) if total >= 6000 else 0
        down_payment = round(total * random.choice([0.1, 0.2, 0.3]), 2) if installment_months else total

        event.update({
            'invoice_number': f"INV-{event_time:%y%m}-{uuid.uuid4().hex[:8].upper()}",
            'sale_type_id': sale_type_id,
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price_egp': float(product['unit_price_egp']),
            'gross_amount_egp': gross,
            'sale_discount_egp': discount,
            'net_before_vat_egp': net_before_vat,
            'sale_vat_egp': vat,
            'total_amount_egp': total,
            'cost_egp': cost,
            'margin_egp': margin,
            'commission_egp': round(max(0.0, margin) * rep['commission_rate_pct'] / 100, 2) if success else 0.0,
            'store_id': store['store_id'],
            'sales_channel_id': store['sales_channel_id'],
            'sales_rep_id': rep['sales_rep_id'],
            'previous_operator_id': random.choice([o for o in OPERATOR_IDS if o != operator_id]) if sale_type_id == 2 else None,
            'sold_imei': str(random.randint(10**14, 10**15 - 1)) if is_device else None,
            'sold_iccid': f'8920{random.randint(0, 10**15 - 1):015d}' if sale['requires_sim'] else None,
            'installment_months': installment_months,
            'down_payment_egp': down_payment,
            'installment_monthly_egp': round((total - down_payment) / installment_months, 2) if installment_months else 0.0,
            'is_returned': success and is_device and random.random() < 0.03,
            'activation_timestamp': (event_time + timedelta(minutes=random.randint(1, 240))).isoformat() if success and sale['requires_sim'] else None,
            'channel_id': store['sales_channel_id'],
        })

    # ======================= CORPORATE ORDER (B2B: companies buying lines) =======================
    else:
        order_status_id = random.choices([1, 2, 3, 4, 5, 6, 7], [5, 12, 8, 15, 45, 8, 7])[0]
        # 4 Approved / 5 Provisioned -> Success, 6 Rejected / 7 Cancelled -> Failed, others -> Pending
        event['status_id'] = 1 if order_status_id in (4, 5) else 2 if order_status_id in (6, 7) else 3
        event['failure_reason_id'] = 9 if order_status_id == 6 else 10 if order_status_id == 7 else 1

        low, high = {'SME': (5, 50), 'Large': (50, 500), 'Enterprise': (500, 5000)}[account['size_tier']]
        lines = random.randint(low, high)
        bulk_discount = (15 if lines >= 1000 else 10 if lines >= 200 else 5 if lines >= 50 else 0) + random.choice([0, 0, 2, 3])
        order_plan = random.choice(POSTPAID_BY_OPERATOR[operator_id])
        fee_per_line = round(order_plan['price_egp'] * (1 - bulk_discount / 100), 2)
        mrc = round(lines * fee_per_line, 2)
        contract_months = CONTRACT_BY_ID[account['contract_type_id']]['duration_months']
        one_time_fees = round(lines * random.choice([0, 15, 25]), 2)
        includes_devices = random.random() < 0.35
        device_count = int(lines * random.uniform(0.2, 1.0)) if includes_devices else 0
        device_value = round(device_count * random.choice([6000, 14000]), 2)

        event.update({
            'order_id': f"ORD-{event_time:%y%m}-{uuid.uuid4().hex[:8].upper()}",
            'po_number': f'PO-{random.randint(100000, 999999)}',
            'company_name': account['company_name'],
            'industry_id': account['industry_id'],
            'company_size_tier': account['size_tier'],
            'plan_id': order_plan['plan_id'],
            'contract_type_id': account['contract_type_id'],
            'payment_method_id': 11,     # Bank Transfer / Invoice
            'channel_id': 5,             # Field Sales (B2B)
            'order_status_id': order_status_id,
            'lines_ordered': lines,
            'lines_provisioned': lines if order_status_id == 5 else int(lines * random.random()) if order_status_id == 4 else 0,
            'bulk_discount_pct': bulk_discount,
            'monthly_fee_per_line_egp': fee_per_line,
            'monthly_recurring_revenue_egp': mrc,
            'one_time_fees_egp': one_time_fees,
            'contract_months': contract_months,
            'payment_terms_id': account['payment_terms_id'],
            'includes_devices': includes_devices,
            'device_count': device_count,
            'device_value_egp': device_value,
            'contract_value_egp': round(mrc * contract_months + one_time_fees + device_value, 2),
            'credit_check_passed': False if order_status_id == 6 else True if order_status_id >= 4 else None,
            'account_manager_id': account['account_manager_id'],
            'sales_rep_id': account['account_manager_id'],
            'store_id': REP_BY_ID[account['account_manager_id']]['store_id'],
            'sales_channel_id': 5,
            'expected_delivery_date': (event_time + timedelta(days=random.randint(3, 30))).date().isoformat(),
        })

    return event


if __name__ == "__main__":
    for t in EVENT_TYPES:
        print("=" * 80)
        print(t.upper())
        print("=" * 80)
        print(json.dumps(generate_telecom_event(t), indent=2, ensure_ascii=False))