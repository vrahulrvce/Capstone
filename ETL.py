from pymongo import MongoClient
import pandas as pd
import re
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
import calendar



df_actual = pd.read_excel("Team5_Heart_Failure.xlsx")
df_common = pd.read_excel("common_name.xlsx")
df = pd.read_excel("Team5_Heart_Failure.xlsx")
df.columns
df.drop(columns= "CMP_name" , inplace= True , errors= "ignore")
df_clist = pd.read_excel("common_name.xlsx")
df_clist.columns
df_clist = df_clist[1:]
list1  = df_common['Unnamed: 0']
list1 = tuple(list1)
rows = []
current_company = None

for _, row in df.iterrows():
    item = str(row["CMP_with_DRUG"]).strip()
    if item in list1:  
        current_company = item
    else:  
        if current_company:
            new_row = row.to_dict() 
            new_row["company"] = current_company
            new_row["drug"] = item
            rows.append(new_row)
            
df_final = pd.DataFrame(rows)
df_final
df_final.drop(columns= 'CMP_with_DRUG' , inplace= True)
meta_cols = ['MTY' , 'company' , 'drug']
value_cols = [col for col in df_final.columns if col not in meta_cols]
df_melted = df_final.melt(id_vars=meta_cols, value_vars=value_cols,
                    var_name='raw_column', value_name='value')
import re
def parse_column(col):
    desc_match = re.match(r'^([A-Z ]+)', col)
    age_match = re.search(r'(\d{2} TO \d{2})', col)
    gender_match = re.search(r'\b(MALE|FEMALE)\b', col)
    icd_match = re.search(r'\b(I\d{4})\b', col)
    desc1_match = re.search(r'- (.+)', col)

    return pd.Series({
        'type_of_drug': desc_match.group(1).strip() if desc_match else None,
        'age_range': age_match.group(1) if age_match else None,
        'gender': gender_match.group(1) if gender_match else None,
        'icd_code': icd_match.group(1) if icd_match else None,
        'description': desc1_match.group(1).replace(' Patient Visits', '') if desc1_match else None
    })
meta_df = df_melted['raw_column'].apply(parse_column)
df_finalver = pd.concat([df_melted.drop(columns='raw_column'), meta_df], axis=1)
df_finalver["MTY"] = pd.to_datetime(df_finalver["MTY"], errors="coerce")
df_finalver["month"] = df_finalver["MTY"].dt.month.fillna(-1).astype(int)
df_finalver["year"] = df_finalver["MTY"].dt.year.fillna(-1).astype(int)
df_finalver = df_finalver.drop(columns=["MTY"])
###df_finalver = df_finalver.where(pd.notnull(df_finalver), None) ONLY TO SUIT MONGODB BUT REMOVING THIS NOW 
### SOLVED AGE_RANGE issue and filled value with 0 vist (decide either to remove or keep em)
df_finalver['age_range'] = df_finalver['age_range'].fillna("85")
df_finalver['value'] = df_finalver['value'].fillna(0)
print("Finished Transforming \n")
############ pushing this to mongo cluster ################
cli = MongoClient("mongodb://localhost:27017/")
db = cli["final_project"]
collection = db["heal_record"]
data = df_finalver.to_dict(orient= "records")
if data :
    collection.insert_many(data)
print(f"✅ Inserted {len(data)} records into {db.name}.{collection.name}")

############ NOW PUSH THE SAME TO CASSANDRA Primary key (PRIMARY KEY ((year, month), company, drug, age_range, gender, icd_code, type_of_drug)) #################

cli = MongoClient("mongodb://localhost:27017/")
db = cli["final_project"]
collection = db["heal_record"]
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
session.set_keyspace("healthcare")
insert_q = session.prepare("""
    INSERT INTO drug_use (
        year, month, company, drug, age_range, gender, icd_code,
        description, type_of_drug, value
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
batch = BatchStatement()
cursor = collection.find({}, no_cursor_timeout=True).batch_size(5000) #### The most optimal for cassandra do not add more .... server error and will crash 
count = 0
for doc in cursor:
    try:
        batch.add(insert_q, (
            int(doc.get("year")) if doc.get("year") else None,
            int(doc.get("month")) if doc.get("month") else None,
            doc.get("company"),
            doc.get("drug"),
            str(doc.get("age_range")) if doc.get("age_range") else None,
            doc.get("gender"),
            doc.get("icd_code"),
            doc.get("description"),
            doc.get("type_of_drug"),
            float(doc["value"]) if doc.get("value") not in [None, ""] else None
        ))
        count += 1
        if count % 100 == 0:
            session.execute(batch)
            batch.clear()
            print(f"Inserted {count} records...")
    except Exception as e:
        print(f"Error inserting doc {doc.get('_id')}: {e}")
if batch:
    session.execute(batch)
cursor.close()
print(f"FINISHED: {count}")

############ Push the cassandra data to postgre and make the ER diagram and calculate the normalization factor ##################

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["final_project"]
collection = mongo_db["heal_record"]
conn = psycopg2.connect(
    dbname="postgres",
    user="student",
    password="student",
    host="localhost",
    port="49168"
)
cur = conn.cursor()
def get_or_insert_time(year, month):
    if not year or not month:
        return None
    cur.execute("SELECT time_id FROM dim_time WHERE year=%s AND month=%s", (year, month))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        month_name = calendar.month_name[month]
        quarter = (month - 1) // 3 + 1
        cur.execute(
            """INSERT INTO dim_time (year, month, month_name, quarter)
               VALUES (%s, %s, %s, %s) RETURNING time_id""",
            (year, month, month_name, quarter)
        )
        return cur.fetchone()[0]
def get_or_insert_company(name):
    if not name:
        return None
    cur.execute("SELECT cmp_id FROM dim_cmp WHERE compnay_name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO dim_cmp (compnay_name) VALUES (%s) RETURNING cmp_id", (name,))
        return cur.fetchone()[0]

def get_or_insert_drug(name):
    if not name:
        return None
    cur.execute("SELECT drug_id FROM dim_drug WHERE drug_name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO dim_drug (drug_name) VALUES (%s) RETURNING drug_id", (name,))
        return cur.fetchone()[0]

def get_or_insert_demo(age_range, gender):
    cur.execute("SELECT demo_id FROM dim_demo WHERE age_range=%s AND gender=%s", (age_range, gender))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute(
            "INSERT INTO dim_demo (age_range, gender) VALUES (%s, %s) RETURNING demo_id",
            (age_range, gender)
        )
        return cur.fetchone()[0]

def get_or_insert_type(type_of_drug):
    if not type_of_drug:
        return None
    cur.execute("SELECT type_d FROM dim_type WHERE type_of_drug=%s", (type_of_drug,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute(
            "INSERT INTO dim_type (type_of_drug) VALUES (%s) RETURNING type_d",
            (type_of_drug,)
        )
        return cur.fetchone()[0]
batch_data = []
count = 0
cursor = collection.find({}, no_cursor_timeout=True).batch_size(5000) ### SET between 5000 and 10000 currently 5000 is a safer limit and 10000 is the peak
for doc in cursor:
    try:
        time_id = get_or_insert_time(
            int(doc.get("year")) if doc.get("year") else None,
            int(doc.get("month")) if doc.get("month") else None
        )
        cmp_id = get_or_insert_company(doc.get("company"))
        drug_id = get_or_insert_drug(doc.get("drug"))
        demo_id = get_or_insert_demo(str(doc.get("age_range")) if doc.get("age_range") else None,
                                     doc.get("gender"))
        type_id = get_or_insert_type(doc.get("type_of_drug"))
        icd_code = doc.get("icd_code")
        description = doc.get("description")
        total_count = int(doc["value"]) if doc.get("value") not in [None, ""] else None

        batch_data.append((time_id, cmp_id, drug_id, demo_id, type_id,
                           icd_code, description, total_count))
        count += 1
        if count % 5000 == 0:           #### change the batch size here too 
            execute_batch(cur, """
                INSERT INTO fact_main (
                    time_id, cmp_id, drug_id, demo_id, type_d,
                    icd_code, description, total_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, batch_data)
            conn.commit()
            batch_data = []
            print(f"Inserted {count} records...")

    except Exception as e:
        print(f"Error inserting doc {doc.get('_id')}: {e}")
        conn.rollback()
if batch_data:
    execute_batch(cur, """
        INSERT INTO fact_main (
            time_id, cmp_id, drug_id, demo_id, type_d,
            icd_code, description, total_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    conn.commit()

cursor.close()
cur.close()
conn.close()
print(f"FINISHED: {count}")