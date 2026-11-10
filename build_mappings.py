import json, os
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mappings")
os.makedirs(out, exist_ok=True)
def dump(name, rows):
    with open(f"{out}/{name}.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

dump("map_operators", [
 {"operator_id":1,"operator":"Vodafone Egypt","operator_ar":"فودافون","prefix":"010","wallet_name":"Vodafone Cash","ussd_code":"*9#"},
 {"operator_id":2,"operator":"Etisalat by e&","operator_ar":"اتصالات","prefix":"011","wallet_name":"Etisalat Cash","ussd_code":"*555#"},
 {"operator_id":3,"operator":"Orange Egypt","operator_ar":"أورنج","prefix":"012","wallet_name":"Orange Cash","ussd_code":"#100#"},
 {"operator_id":4,"operator":"WE (Telecom Egypt)","operator_ar":"وي","prefix":"015","wallet_name":"WE Pay","ussd_code":"*688#"},
])

govs = [
 ("Cairo","القاهرة","Greater Cairo",30.0444,31.2357),("Giza","الجيزة","Greater Cairo",30.0131,31.2089),
 ("Qalyubia","القليوبية","Greater Cairo",30.4103,31.2078),("Alexandria","الإسكندرية","Alexandria & Delta",31.2001,29.9187),
 ("Beheira","البحيرة","Alexandria & Delta",31.0341,30.4682),("Gharbia","الغربية","Alexandria & Delta",30.8754,31.0335),
 ("Dakahlia","الدقهلية","Alexandria & Delta",31.0409,31.3785),("Sharqia","الشرقية","Alexandria & Delta",30.5877,31.502),
 ("Monufia","المنوفية","Alexandria & Delta",30.5972,30.9876),("Kafr El Sheikh","كفر الشيخ","Alexandria & Delta",31.3085,30.9399),
 ("Damietta","دمياط","Alexandria & Delta",31.4165,31.8133),("Port Said","بورسعيد","Canal & Sinai",31.2653,32.3019),
 ("Ismailia","الإسماعيلية","Canal & Sinai",30.5965,32.2715),("Suez","السويس","Canal & Sinai",29.9668,32.5498),
 ("North Sinai","شمال سيناء","Canal & Sinai",31.1316,33.7984),("South Sinai","جنوب سيناء","Canal & Sinai",28.2362,33.6253),
 ("Fayoum","الفيوم","Upper Egypt",29.3084,30.8428),("Beni Suef","بني سويف","Upper Egypt",29.0661,31.0994),
 ("Minya","المنيا","Upper Egypt",28.1099,30.7503),("Assiut","أسيوط","Upper Egypt",27.1809,31.1837),
 ("Sohag","سوهاج","Upper Egypt",26.5591,31.6957),("Qena","قنا","Upper Egypt",26.1551,32.716),
 ("Luxor","الأقصر","Upper Egypt",25.6872,32.6396),("Aswan","أسوان","Upper Egypt",24.0889,32.8998),
 ("Red Sea","البحر الأحمر","Red Sea & Desert",27.2579,33.8116),("Matrouh","مطروح","Red Sea & Desert",31.3543,27.2373),
 ("New Valley","الوادي الجديد","Red Sea & Desert",25.4519,30.5464),
]
dump("map_governorates", [{"governorate_id":i+1,"governorate":g[0],"governorate_ar":g[1],"region":g[2],"latitude":g[3],"longitude":g[4]} for i,g in enumerate(govs)])

# (operator_id, name, type, price, data_gb, minutes, sms, validity_days)  -- fictional pricing for simulation only
plans = [
 (1,"Vodafone Flex 70","Prepaid",70,10,500,100,30),(1,"Vodafone Flex 150","Prepaid",150,30,1500,300,30),
 (1,"Vodafone Red 400","Postpaid",400,60,3000,1000,30),(1,"Vodafone Net Only 100","Data-only",100,50,0,0,30),
 (2,"Etisalat Fakka 60","Prepaid",60,8,400,100,30),(2,"Etisalat Super Kix 140","Prepaid",140,28,1400,300,30),
 (2,"Etisalat Hekaya 350","Postpaid",350,55,2500,800,30),(2,"Etisalat Home Net 120","Data-only",120,60,0,0,30),
 (3,"Orange Go 65","Prepaid",65,9,450,100,30),(3,"Orange Shabab 130","Prepaid",130,25,1200,250,30),
 (3,"Orange Dream 380","Postpaid",380,58,2800,900,30),(3,"Orange Net 110","Data-only",110,55,0,0,30),
 (4,"WE Mix 55","Prepaid",55,7,350,80,30),(4,"WE Super 120","Prepaid",120,22,1000,200,30),
 (4,"WE Gold 300","Postpaid",300,50,2200,700,30),(4,"WE Home Internet 200","Data-only",200,140,0,0,30),
]
dump("map_plans", [{"plan_id":i+1,"operator_id":p[0],"plan_name":p[1],"plan_type":p[2],"price_egp":p[3],"data_gb":p[4],"voice_minutes":p[5],"sms_count":p[6],"validity_days":p[7]} for i,p in enumerate(plans)])

offers = [
 ("No offer","None",0,0,0,None),("Double Balance Weekend","Balance bonus",0,0,0,None),
 ("Ramadan Bonus Data","Bonus data",0,5,0,None),("Cashback 10%","Cashback",0,0,10,None),
 ("Student Discount 15%","Discount",15,0,0,None),("Eid Mega Offer","Bonus data",0,10,0,None),
 ("First Recharge Gift","Bonus data",0,2,0,None),("Wallet Cashback 20 EGP","Cashback",0,0,20,None),
 ("Loyalty Points 2x","Loyalty",0,0,0,None),("Black Friday 30% Off","Discount",30,0,0,None),
 ("Vodafone Cash Fridays","Cashback",0,0,15,1),("Etisalat Cash Rewards","Cashback",0,0,15,2),
 ("Orange Cash Bonus","Cashback",0,0,15,3),("WE Pay Cashback","Cashback",0,0,15,4),
]
dump("map_offers", [{"offer_id":i+1,"offer_name":o[0],"offer_type":o[1],"discount_pct":o[2],"bonus_data_gb":o[3],"cashback_egp":o[4],"operator_id":o[5]} for i,o in enumerate(offers)])

dump("map_service_types", [
 {"service_type_id":1,"service_type":"Voice Call","unit":"second"},{"service_type_id":2,"service_type":"Mobile Data","unit":"MB"},
 {"service_type_id":3,"service_type":"SMS","unit":"message"},{"service_type_id":4,"service_type":"International Roaming","unit":"MB"},
 {"service_type_id":5,"service_type":"Value Added Service","unit":"subscription"}])

dump("map_payment_methods", [
 {"payment_method_id":1,"payment_method":"Vodafone Cash","is_wallet":True,"is_card":False,"requires_auth":True},
 {"payment_method_id":2,"payment_method":"Etisalat Cash","is_wallet":True,"is_card":False,"requires_auth":True},
 {"payment_method_id":3,"payment_method":"Orange Cash","is_wallet":True,"is_card":False,"requires_auth":True},
 {"payment_method_id":4,"payment_method":"WE Pay","is_wallet":True,"is_card":False,"requires_auth":True},
 {"payment_method_id":5,"payment_method":"InstaPay","is_wallet":False,"is_card":False,"requires_auth":True},
 {"payment_method_id":6,"payment_method":"Fawry","is_wallet":False,"is_card":False,"requires_auth":False},
 {"payment_method_id":7,"payment_method":"Credit Card","is_wallet":False,"is_card":True,"requires_auth":True},
 {"payment_method_id":8,"payment_method":"Debit Card","is_wallet":False,"is_card":True,"requires_auth":True},
 {"payment_method_id":9,"payment_method":"Scratch Card","is_wallet":False,"is_card":False,"requires_auth":False},
 {"payment_method_id":10,"payment_method":"Cash (Retail Store)","is_wallet":False,"is_card":False,"requires_auth":False},
 {"payment_method_id":11,"payment_method":"Bank Transfer / Invoice","is_wallet":False,"is_card":False,"requires_auth":True}])

dump("map_transaction_statuses", [
 {"status_id":1,"status":"Success","is_successful":True},{"status_id":2,"status":"Failed","is_successful":False},
 {"status_id":3,"status":"Pending","is_successful":False}])

dump("map_failure_reasons", [
 {"failure_reason_id":1,"failure_reason":None},{"failure_reason_id":2,"failure_reason":"Insufficient balance"},
 {"failure_reason_id":3,"failure_reason":"Network error"},{"failure_reason_id":4,"failure_reason":"Invalid number"},
 {"failure_reason_id":5,"failure_reason":"Invalid or used voucher"},{"failure_reason_id":6,"failure_reason":"Payment declined"},
 {"failure_reason_id":7,"failure_reason":"Wallet limit exceeded"},{"failure_reason_id":8,"failure_reason":"Timeout"},
 {"failure_reason_id":9,"failure_reason":"Credit check failed"},{"failure_reason_id":10,"failure_reason":"Incomplete KYC documents"},
 {"failure_reason_id":11,"failure_reason":"Number not eligible for port-in"},{"failure_reason_id":12,"failure_reason":"Out of stock"}])

dump("map_wallet_transaction_types", [
 {"wallet_txn_type_id":1,"wallet_txn_type":"Send Money","charges_fee":True},
 {"wallet_txn_type_id":2,"wallet_txn_type":"Cash In","charges_fee":False},
 {"wallet_txn_type_id":3,"wallet_txn_type":"Cash Out (ATM)","charges_fee":True},
 {"wallet_txn_type_id":4,"wallet_txn_type":"Bill Payment","charges_fee":True},
 {"wallet_txn_type_id":5,"wallet_txn_type":"Merchant Payment (QR)","charges_fee":False},
 {"wallet_txn_type_id":6,"wallet_txn_type":"Airtime Top-up","charges_fee":False}])

dump("map_channels", [
 {"channel_id":1,"channel":"Mobile App"},{"channel_id":2,"channel":"USSD"},{"channel_id":3,"channel":"Retail Store"},
 {"channel_id":4,"channel":"Fawry Kiosk"},{"channel_id":5,"channel":"Website"},{"channel_id":6,"channel":"Call Center"}])

dump("map_customer_segments", [
 {"segment_id":1,"segment":"Individual"},{"segment_id":2,"segment":"Youth"},{"segment_id":3,"segment":"Corporate"},{"segment_id":4,"segment":"VIP"}])

dump("map_devices", [
 {"device_id":1,"brand":"Samsung","device_type":"Smartphone","os":"Android"},{"device_id":2,"brand":"Oppo","device_type":"Smartphone","os":"Android"},
 {"device_id":3,"brand":"Xiaomi","device_type":"Smartphone","os":"Android"},{"device_id":4,"brand":"Apple iPhone","device_type":"Smartphone","os":"iOS"},
 {"device_id":5,"brand":"Realme","device_type":"Smartphone","os":"Android"},{"device_id":6,"brand":"Infinix","device_type":"Smartphone","os":"Android"},
 {"device_id":7,"brand":"Huawei","device_type":"Smartphone","os":"Android"},{"device_id":8,"brand":"Nokia","device_type":"Feature Phone","os":"KaiOS"},
 {"device_id":9,"brand":"Huawei 4G Router","device_type":"Router","os":"Embedded"}])

dump("map_network_types", [
 {"network_type_id":1,"network_type":"3G"},{"network_type_id":2,"network_type":"4G"},{"network_type_id":3,"network_type":"4.5G"},{"network_type_id":4,"network_type":"5G"}])


# ====================== SALES & CORPORATE (B2B) ======================
import random
rng = random.Random(11)

dump("map_loyalty_tiers", [
 {"loyalty_tier_id":1,"loyalty_tier":"Bronze","min_points":0},{"loyalty_tier_id":2,"loyalty_tier":"Silver","min_points":2000},
 {"loyalty_tier_id":3,"loyalty_tier":"Gold","min_points":6000},{"loyalty_tier_id":4,"loyalty_tier":"Platinum","min_points":15000}])

dump("map_contract_types", [
 {"contract_type_id":1,"contract_type":"Prepaid (No Contract)","duration_months":0,"is_corporate":False},
 {"contract_type_id":2,"contract_type":"Postpaid 12M","duration_months":12,"is_corporate":False},
 {"contract_type_id":3,"contract_type":"Postpaid 24M","duration_months":24,"is_corporate":False},
 {"contract_type_id":4,"contract_type":"Corporate Pool 12M","duration_months":12,"is_corporate":True},
 {"contract_type_id":5,"contract_type":"Corporate Pool 24M","duration_months":24,"is_corporate":True},
 {"contract_type_id":6,"contract_type":"Corporate Fleet 36M","duration_months":36,"is_corporate":True}])

dump("map_payment_terms", [
 {"payment_terms_id":1,"payment_terms":"Prepaid / Immediate","days":0},{"payment_terms_id":2,"payment_terms":"Net 15","days":15},
 {"payment_terms_id":3,"payment_terms":"Net 30","days":30},{"payment_terms_id":4,"payment_terms":"Net 60","days":60},
 {"payment_terms_id":5,"payment_terms":"Monthly Direct Debit","days":30}])

dump("map_order_statuses", [
 {"order_status_id":1,"order_status":"Draft","is_final":False},{"order_status_id":2,"order_status":"Submitted","is_final":False},
 {"order_status_id":3,"order_status":"Credit Check","is_final":False},{"order_status_id":4,"order_status":"Approved","is_final":False},
 {"order_status_id":5,"order_status":"Provisioned","is_final":True},{"order_status_id":6,"order_status":"Rejected","is_final":True},
 {"order_status_id":7,"order_status":"Cancelled","is_final":True}])

dump("map_sales_channels", [
 {"sales_channel_id":1,"sales_channel":"Own Store","is_b2b":False},{"sales_channel_id":2,"sales_channel":"Authorized Dealer","is_b2b":False},
 {"sales_channel_id":3,"sales_channel":"Online Shop","is_b2b":False},{"sales_channel_id":4,"sales_channel":"Telesales","is_b2b":False},
 {"sales_channel_id":5,"sales_channel":"Field Sales (B2B)","is_b2b":True}])

dump("map_sale_types", [
 {"sale_type_id":1,"sale_type":"New Line","requires_sim":True},{"sale_type_id":2,"sale_type":"MNP Port-in","requires_sim":True},
 {"sale_type_id":3,"sale_type":"SIM Replacement","requires_sim":True},{"sale_type_id":4,"sale_type":"Device Sale","requires_sim":False},
 {"sale_type_id":5,"sale_type":"Upgrade (Device + Plan)","requires_sim":False},{"sale_type_id":6,"sale_type":"VIP Number","requires_sim":True}])

# (name, category, unit_price, unit_cost)  -- fictional prices for simulation only
prods = [("Prepaid SIM Card","SIM",15,5),("Postpaid SIM Card","SIM",0,3),("eSIM Activation","SIM",30,2),
 ("SIM Replacement","SIM",25,3),("VIP / Golden Number","Number",500,20),
 ("Smartphone - Entry","Device",6000,5000),("Smartphone - Mid","Device",14000,12200),("Smartphone - Premium","Device",38000,34000),
 ("4G Router","Device",1500,1200),("5G Home Router","Device",3200,2700),("MiFi 4G Pocket","Device",1100,900),
 ("Tablet","Device",7500,6500),("Accessory Bundle","Accessory",350,150)]
dump("map_products", [{"product_id":i+1,"product_name":p[0],"product_category":p[1],"unit_price_egp":p[2],"unit_cost_egp":p[3]} for i,p in enumerate(prods)])

dump("map_industries", [{"industry_id":i+1,"industry":n} for i,n in enumerate(
 ["Banking & Finance","Retail","Healthcare","Education","Manufacturing","Oil & Gas","Government","Hospitality & Tourism",
  "Logistics & Transport","IT & Software","Real Estate & Construction","Media & Advertising"])])

# ---- stores / points of sale
OPSHORT = {1:"Vodafone",2:"Etisalat",3:"Orange",4:"WE"}
stores = []
def add_store(name, op, gov, ch, size):
    stores.append({"store_id":len(stores)+1,"store_name":name,"operator_id":op,"governorate_id":gov,"sales_channel_id":ch,
                   "store_size":size,"monthly_target_egp":{"Small":150000,"Medium":400000,"Flagship":1200000}[size] if ch<3 else 2500000 if ch==3 else 800000 if ch==4 else 5000000,
                   "opened_date":f"{rng.randint(2012,2025)}-{rng.randint(1,12):02d}-01"})
big = [1,2,4,7,8]   # Cairo, Giza, Alex, Dakahlia, Sharqia ids = governorate_id
for op in OPSHORT:
    add_store(f"{OPSHORT[op]} Online Shop",op,1,3,"Flagship")
    add_store(f"{OPSHORT[op]} Telesales Center",op,1,4,"Medium")
    add_store(f"{OPSHORT[op]} Corporate Sales Office",op,1,5,"Medium")
    add_store(f"{OPSHORT[op]} Corporate Sales Office - Alexandria",op,4,5,"Small")
    for gid in range(1,28):
        n = 3 if gid in (1,2) else 2 if gid in (4,7,8,3) else 1
        for k in range(n):
            ch = 1 if rng.random()<0.45 else 2
            size = rng.choice(["Small","Medium","Medium","Flagship"]) if ch==1 else "Small"
            gname = govs[gid-1][0]
            add_store(f"{OPSHORT[op]} {'Store' if ch==1 else 'Dealer'} - {gname} #{k+1}",op,gid,ch,size)
dump("map_stores", stores)

# ---- sales reps
first = ["Ahmed","Mohamed","Mahmoud","Omar","Youssef","Ali","Hassan","Khaled","Mostafa","Ibrahim","Karim","Tarek","Amr","Hossam","Fatma","Mariam","Nour","Salma","Aya","Heba","Menna","Rana","Yasmin","Dina","Mona","Sara","Reem","Esraa","Hagar","Nada"]
last = ["Hassan","Ibrahim","Salah","Abdelrahman","Mostafa","Ahmed","Mahmoud","ElSayed","Fathy","Nabil","Gamal","Farouk","Samir","Hamdy","Shaker","Zaki","Abdallah","Kamel","Soliman","Metwally","ElShazly","Badawy"]
team = {1:"Retail",2:"Dealer",3:"Online",4:"Telesales",5:"B2B"}
reps = []
for st in stores:
    cnt = {1:rng.randint(2,5),2:1,3:6,4:6,5:4}[st["sales_channel_id"]]
    for _ in range(cnt):
        reps.append({"sales_rep_id":len(reps)+1,"sales_rep_name":f"{rng.choice(first)} {rng.choice(last)}","store_id":st["store_id"],
                     "operator_id":st["operator_id"],"team":team[st["sales_channel_id"]],"is_b2b":st["sales_channel_id"]==5,
                     "hire_date":f"{rng.randint(2015,2026)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
                     "commission_rate_pct":rng.choice([3,4,5,6]) if st["sales_channel_id"]!=5 else rng.choice([1.5,2,2.5])})
dump("map_sales_reps", reps)

# ---- corporate accounts (fictional company names)
pre = ["Nile","Delta","Pyramid","Sphinx","Lotus","Horus","Karnak","Giza","Cairo","Alex","Suez","Sinai","Red Sea","Luxor","Aswan","Memphis","Amun","Ramses","Orbit","Falcon","Zenith","Atlas","Vertex","Nova","Pharos"]
ind_suf = {1:["Capital","Finance Group","Investment"],2:["Retail","Markets","Stores"],3:["Medical Center","Pharma","Clinics"],4:["Academy","Schools","University"],
  5:["Industries","Manufacturing","Steel"],6:["Petroleum","Energy","Drilling"],7:["Authority","Agency","Directorate"],8:["Hotels & Resorts","Travel","Tourism"],
  9:["Logistics","Shipping","Transport"],10:["Technologies","Software","Systems"],11:["Real Estate","Developments","Construction"],12:["Media","Advertising","Studios"]}
tiers = [("SME",50,(5,200),(20,50)),("Large",35,(200,2000),(50,500)),("Enterprise",15,(2000,30000),(500,5000))]
accts = []
for i in range(80):
    t = rng.choices(tiers,[x[1] for x in tiers])[0]
    ind = rng.randint(1,12)
    op = rng.choices([1,2,3,4],[40,28,22,10])[0]
    gov = rng.choices(range(1,28),[8 if g==1 else 6 if g==2 else 5 if g==4 else 1 for g in range(1,28)])[0]
    lines = rng.randint(*t[3])
    accts.append({"corporate_account_id":i+1,"company_name":f"{rng.choice(pre)} {rng.choice(ind_suf[ind])} {i+1:02d}","industry_id":ind,
      "size_tier":t[0],"employees":rng.randint(*t[2]),"governorate_id":gov,"primary_operator_id":op,
      "contract_type_id":rng.choice([4,5,6]),"payment_terms_id":rng.choice([2,3,3,4,5]),
      "credit_limit_egp":lines*rng.choice([300,500,800]),"lines_contracted":lines,
      "tax_id":f"{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(100,999)}","commercial_register_no":f"CR-{rng.randint(100000,999999)}",
      "account_manager_id":rng.choice([r for r in reps if r["is_b2b"] and r["operator_id"]==op])["sales_rep_id"],
      "contact_name":f"{rng.choice(first)} {rng.choice(last)}","contact_msisdn":f"+20{ {1:'10',2:'11',3:'12',4:'15'}[op] }{rng.randint(0,99999999):08d}",
      "contract_start_date":f"{rng.randint(2019,2026)}-{rng.randint(1,12):02d}-01"})
dump("map_corporate_accounts", accts)
print(sorted(os.listdir(out)))
